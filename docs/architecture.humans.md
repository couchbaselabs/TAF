# TAF Architecture Overview

## Executive Summary

TAF (Test Automation Framework) is Couchbase's primary test automation system for validating Couchbase Server and Capella functionality. It provides a Python-based infrastructure for testing KV operations, storage engines, analytics, security, upgrades, and cross-component features across on-premise, dedicated, and serverless deployment models.

**Architecture Philosophy:**
- Component-based test organization mirroring Couchbase Server services
- Flexible base test class selection via `runtype` parameter
- REST API + Python SDK hybrid approach for cluster interactions
- Extensive helper libraries for common operations

**Scale:** ~40+ component directories, 1000+ test modules, supporting multiple Couchbase versions and deployment models.

## High-Level Component Architecture

### 1. Test Execution Layer (`testrunner.py`)
**Responsibility:** Framework entry point and test orchestration.

**Key Functions:**
- Command-line argument parsing (`-i`, `-c`, `-t`, `-p`, etc.)
- Test configuration loading from `.ini` and `.conf` files
- Base test class selection based on environments
- Test discovery and execution via unittest framework
- Logging and result collection

**Runtime Flow:**
1. Parse command-line arguments for cluster config (`node.ini`) and test selection (`*.conf`)
2. Load cluster topology and credentials via `TestInputSingleton`
3. Select appropriate base test class based on `runtype` parameter
4. Execute tests with configuration parameters
5. Collect results and logs to timestamped output directories

### 2. Base Test Class Factory (`pytests/basetestcase.py`)
**Responsibility:** Provides dynamic base class selection for different deployment models.

**Class Mapping:**
- `runtype="default"` → `OnPremBaseTest` – On-premise Couchbase Server
- `runtype="dedicated"` → `ProvisionedBaseTestCase` – Capella dedicated clusters
- `runtype="serverless"` → `OnCloudBaseTest` – Capella serverless
- `runtype="columnar"` → `ColumnarBaseTest` – Columnar analytics service

**Purpose:** Allows the same test code to run across different Couchbase deployment models by abstracting environment-specific setup/teardown.

### 3. Component Test Suite (`pytests/`)
**Structure:** 40+ component directories, each containing tests for specific Couchbase Server services or features.

**Major Components:**
- `epengine/` – KV/Data Service operations, durability, document handling
- `cbas/` – Analytics Service (CBAS), external datasets, linking, UDFs
- `security/` – TLS, encryption, RBAC, LDAP, SSO, certificates
- `storage/magma/` – Magma storage engine tests (compaction, DGM, rollback)
- `upgrade/` – Version upgrade validation and compatibility matrix
- `Capella/` – Capella REST API v4 endpoint testing
- `Atomicity/` – ACID transaction validation

**Test Convention:**
- Each directory inherits from appropriate base test class
- Tests use `TestInputSingleton.input.param()` for parameter access
- Follow PEP8 Python standards

### 4. Core Framework Libraries (`lib/`)
**Purpose:** Shared functionality available to all tests.

**Key Libraries:**
- **SDK Client** (`lib/sdk_client3.py`): Python SDK operations wrapper (72KB)
  - CRUD operations, durability, transaction support
  - Heavily used across all component tests

- **Framework Library** (`lib/framework_lib/`): Test runner core logic
  - Command-line parsing, test configuration
  - XUnit result formatting and XML output

- **Couchbase Helpers** (`lib/couchbase_helper/`): High-level operations
  - Cluster management, document generation
  - Query helpers, test data creation

- **Feature-Specific Libraries:**
  - `lib/BucketLib/` – Bucket operations
  - `lib/CbasLib/` – Columnar/Analytics operations
  - `lib/SecurityLib/` – Security utilities
  - `lib/SystemEventLogLib/` – Event log validation

### 5. Feature Utilities (`couchbase_utils/`)
**Purpose:** Direct mappings to Couchbase REST API endpoints by feature.

**Organization:** Mirrors Couchbase Server structure
- `couchbase_utils/cb_server_rest_util/cluster_nodes/` – Cluster management
- `couchbase_utils/cb_server_rest_util/buckets/` – Bucket operations
- `couchbase_utils/cb_server_rest_util/index/` – Index service
- `couchbase_utils/cb_server_rest_util/query/` – Query service
- `couchbase_utils/cb_server_rest_util/fts/` – Search service
- `couchbase_utils/cb_server_rest_util/xdcr/` – XDCR replication
- `couchbase_utils/cb_server_rest_util/security/` – Security configurations

**Purpose:** Provides direct access to REST API endpoints without needing to know URLs manually.

### 6. Infrastructure Utilities (`platform_utils/`)
**Purpose:** Low-level infrastructure operations.

**Key Components:**
- **SSH Utilities** (`platform_utils/ssh_util/`): Paramiko-based SSH management
  - Remote command execution
  - File transfer operations
  - Multi-node coordination

- **Error Simulation** (`platform_utils/error_simulation/`): Network/disk failure testing
  - Network partition simulation
  - Disk failure simulation

- **Docker Utilities** (`platform_utils/docker_utils/`): Container operations

### 7. Configuration System
**Purpose:** Test parametrization and cluster configuration.

**File Types:**
- **.ini files** – Cluster topology and credentials
  - Node IPs, SSH credentials
  - Service allocation per node
  - Couchbase admin credentials

- **.conf files** – Test suite selections
  - Test module patterns
  - Parameter passing
  - Format: `module.class.test_method,param1=value1`

### 8. Submodule Architecture
**Critical:** These are maintained separately and MUST NOT be modified.

**Submodules:**
1. **DocLoader** – Java-based REST document loader
   - HTTP-based document generation for load testing
   - Accessed via `--launch_java_doc_loader` flag and `load_docs_using=sirius_java_sdk`

2. **lib/capellaAPI** – Capella REST API libraries
   - Python libraries for Capella management APIs
   - Used in `pytests/Capella/` tests

3. **sirius** – Go-based document client framework
   - Alternative document loading mechanism
   - Accessed via `--launch_sirius_process` or `--launch_sirius_docker`

## Test Workflow Map to Architecture

### 1. Test Discovery Phase
**User Action:** Define test in `.conf` file
**Framework Action:** Parse `.conf` and discover test methods
**Architecture:** `lib/framework_lib/framework.py` → TestLoader discovers test classes

### 2. Cluster Setup Phase
**User Action:** Provide cluster config in `node.ini`
**Framework Action:** Load cluster topology via `TestInputSingleton`
**Architecture:** `lib/couchbase_helper/cluster.py` → Cluster initialization

### 3. Environment Selection Phase
**User Action:** Set `runtype` parameter
**Framework Action:** Select base test class in `pytests/basetestcase.py`
**Architecture:** `basetestcase.py` → OnPremBaseTest/OnCloudBaseTest etc.

### 4. Test Execution Phase
**User Action:** Test runner executes test methods
**Architecture Flow:**
```
Test Class Method
  ↓
lib/sdk_client3.py (Python SDK operations)
  ↓
couchbase_utils/cb_server_rest_util/ (REST API operations)
  ↓
Couchbase Server Cluster
```

### 5. Data Loading Phase
**User Action:** Configure `load_docs_using` parameter with optional flags
**Architecture Options:**
- `default_loader` → `lib/sdk_client3.py` (no external process)
- `sirius_java_sdk` → `DocLoader/` (Java subprocess)
- `sirius_go_sdk` → `sirius/` (Go subprocess)

## Tradeoffs and Constraints

### 1. Deployment Model Abstraction
**Tradeoff:** Single test codebase for multiple deployment models
**Benefit:** Reduced test maintenance, consistent test coverage
**Constraint:** Base test class must be chosen correctly via `runtype`

### 2. Hybrid API Approach
**Tradeoff:** REST API + Python SDK dual approach
**Benefit:** REST API for management operations, SDK for data operations
**Constraint Requires understanding of when to use each

### 3. Document Loading Flexibility
**Tradeoff:** Three different document loading mechanisms
**Benefit:** Can choose optimal loading method per test requirements
**Constraint:** Complex parameter combinations and external process management

### 4. Configuration File Separation
**Tradeoff:** `.ini` for cluster, `.conf` for tests
**Benefit:** Clear separation of concerns
**Constraint:** Must maintain both file types and ensure consistency

## Known Weak Spots

### 1. No Linting/Type Checking
**Issue:** No configured linters or type checkers
**Impact:** Code quality relies on manual review
**Mitigation:** PEP8 guidelines in skill documentation

### 2. Dependency Pinning
**Issue:** `requirements.txt` uses unpinned versions
**Impact:** Repducibility challenges across environments
**Mitigation:** Manual dependency management

### 3. No Coverage Thresholds
**Issue:** No automated code coverage enforcement
**Impact:** Coverage gaps may go undetected
**Mitigation:** Ad-hoc coverage checks

### 4. Submodule Management Complexity
**Issue:** Multiple submodules requiring separate maintenance
**Impact:** Dependency management overhead
**Mitigation:** Clearly documented constraints against modification

### 5. Test Parameter Complexity
**Issue:** Extensive parameter passing via command line and config files
**Impact:** Parameter documentation needs constant updates
**Mitigation:** Comprehensive AGENTS.md and skill documentation

## Multi-Dimensional Testing Support

### Couchbase Service Alignment
TAF architecture aligns with Couchbase Server service structure:

**Test Directories → Couchbase Services:**
- `epengine/` → Data Service (kv)
- `cbas/` → Analytics Service (cbas)
- `security/` → Security features (TLS, RBAC, encryption)
- `index/` → Index Service
- Columnar tests → Columnar features
- FT tests → Search Service
- Eventing tests → Eventing Service
- Backup tests → Backup Service

**Service-Independent Testing:**
Tests can focus on specific services by:
1. Selecting appropriate base test class
2. Configuring cluster with required services in `.ini`
3. Using service-specific helper libraries

## Deployment Model Branching

### On-Premise Testing
**Base Class:** `OnPremBaseTest` (default)
**Configuration:** Direct cluster access, SSH to nodes
**Use Cases:** Storage engine tests, upgrade tests, performance tests

### Capella Dedicated Testing
**Base Class:** `ProvisionedBaseTestCase`
**Configuration:** Capella API integration, cloud cluster management
**Use Cases:** API contract testing, Capella feature validation

### Serverless Testing
**Base Class:** `OnCloudBaseTest`
**Configuration:** Serverless-specific resource management, metering
**Use Cases:** Tenant management, throttling, feature validation

### Columnar Analytics Testing
**Base Class:** `ColumnarBaseTest`
**Configuration:** Columnar service integration, external datasets
**Use Cases:** Analytics operations, data ingestion, query performance

## Document Architecture

### Code Organization Principles
1. **Separation of Concerns:** Framework code in `lib/`, test code in `pytests/`
2. **Feature-Based Structure:** Utilities organized by Couchbase feature
3. **Hierarchical Configuration:** `.ini` files for cluster, `.conf` for tests
4. **Base Class Abstraction:** Different deployment models share test code

### Dependency Management
**Framework Dependencies:** Test operations, cluster management
**Test Dependencies:** Component-specific functionality
**External Dependencies:** Couchbase Server cluster, Capella account

### Extension Points
**Adding New Components:** Create directory in `pytests/` following existing patterns
**Adding New Helper:** Add to appropriate `couchbase_utils/` subdirectory
**Adding New Tests:** Follow conventions in existing component directories

This architecture enables TAF to scale across Couchbase Server's multiple services and deployment models while maintaining test consistency and reducing duplication.
