# TAF Build and Test Matrix

## Core Commands

### Dependency Installation
```bash
# Install Python dependencies
python -m pip install -r requirements.txt

# Initialize submodules
git submodule init
git submodule update --init --force --remote
```

### Test Execution (Primary)

#### Full Suite Execution
```bash
# Run test suite from configuration file
python testrunner.py -i node.ini -c conf/sanity.conf

# With global parameters
python testrunner.py -i node.ini -c conf/collections/collections_rebalance.conf -p get-cbcollect-info=True,skip_cluster_reset=False
```

#### Individual Test Execution
```bash
# Run single test with parameters
python testrunner.py -i node.ini -t epengine.basic_ops.basic_ops.test_doc_size,nodes_init=1 -p durability=MAJORITY
```

### No-Op (Test Discovery)
```bash
# List tests without executing
python testrunner.py -i node.ini -c conf/sanity.conf -n
```

## Special Execution Modes

### Document Loading Options

**Document Loading Parameters:**
- `load_docs_using=default_loader` – Uses built-in Python SDK loader (default value, no additional flags needed)
- `load_docs_using=sirius_java_sdk` – Uses Sirius Java SDK via DocLoader (requires `--launch_java_doc_loader`)
- `load_docs_using=sirius_go_sdk` – Uses Sirius Go SDK via sirius submodule (requires `--launch_sirius_process` or `--launch_sirius_docker`)

**Using built-in Python SDK (default):**
```bash
# Default document loading behavior
python testrunner.py -i node.ini -c conf/sanity.conf

# Explicitly specify default loader (optional)
python testrunner.py -i node.ini -c conf/sanity.conf -p load_docs_using=default_loader
```

**Using Sirius Java SDK via DocLoader:**
```bash
# Start Java-based REST doc loader and use sirius_java_sdk for document loading
python testrunner.py -c conf/sanity.conf -i node.ini -p rerun=False,get-cbcollect-info=False,skip_cluster_reset=True,load_docs_using=sirius_java_sdk --launch_java_doc_loader --sirius_url http://localhost:8080
```

**Using Sirius Go SDK via sirius submodule:**
```bash
# Launch Sirius process client and use sirius_go_sdk for document loading
python testrunner.py -c conf/sanity.conf -i node.ini -p load_docs_using=sirius_go_sdk --launch_sirius_process --sirius_url http://localhost:8080

# Launch Sirius Docker client and use sirius_go_sdk for document loading
python testrunner.py -c conf/sanity.conf -i node.ini -p load_docs_using=sirius_go_sdk --launch_sirius_docker --sirius_url http://localhost:8080
```

**DocLoader usage parameters:**
- `load_docs_using=sirius_java_sdk` – Use Sirius Java SDK (requires `--launch_java_doc_loader`)
- `load_docs_using=default_loader` – Use built-in Python SDK (default)
- `--launch_java_doc_loader` – Launch DocLoader process within test execution
- `--sirius_url <url>` – DocLoader REST endpoint (e.g., http://localhost:8080)
- `rerun=False` – Prevent test reruns when using DocLoader
- `skip_cluster_reset=True` – Preserve cluster state between tests
- `get-cbcollect-info=False` – Skip log collection if not needed

**Manual DocLoader (separate process):**
```bash
# Start DocLoader standalone (in DocLoader directory)
cd DocLoader
mvn install
java -cp ./target/magmadocloader/magmadocloader.jar RestServer.RestApplication --server.port=8080

# Then run TAF without --launch_java_doc_loader
python testrunner.py -c conf/sanity.conf -i node.ini -p load_docs_using=sirius_java_sdk
```

## Build Commands

### Packaging
```bash
# Package distribution tarball
make TAF

# Clean build artifacts
make clean
```

## Component-Specific Test Flows

### KV Engine Tests (epengine)
```bash
# Basic operations
python testrunner.py -i node.ini -c conf/ep_engine/basic_ops.conf

# Durability
python testrunner.py -i node.ini -c conf/ep_engine/durability_success.conf

# Document keys
python testrunner.py -i node.ini -c conf/ep_engine/documentkeys.conf
```

### Storage Engine Tests (Magma/Plasma/Fusion)
```bash
# Magma basic tests
python testrunner.py -i node.ini -c conf/magma/magma_sanity.conf

# Magma DGM (Data-Get-More)
python testrunner.py -i node.ini -c conf/magma/dgm_collections_rebalance.conf

# Fusion tests
python testrunner.py -i node.ini -c conf/fusion/fusion_sanity.conf
```

### Collections Tests (Collections/Scopes)
```bash
# Collections steady state
python testrunner.py -i node.ini -c conf/collections/steady_state.conf

# Collections rebalance
python testrunner.py -i node.ini -c conf/collections/collections_rebalance.conf

# Collections failover
python testrunner.py -i node.ini -c conf/collections/collections_autofailover.conf
```

### Columnar/Analytics Tests (CBAS)
```bash
# Columnar basic tests
python testrunner.py -i node.ini -c conf/columnar/copy_to_kv.conf

# CBAS tests
python testrunner.py -i node.ini -c conf/cbas/py-cbas-collections.conf

# Columnar on-prem
python testrunner.py -i node.ini -c conf/columnar/onprem/onprem_system_test.conf
```

### Security Tests
```bash
# TLS encryption
python testrunner.py -i node.ini -c conf/ns_server/n2n_encryption_x509.conf

# RBAC
python testrunner.py -i node.ini -c conf/security/py-rbac_test.conf

# JWT authentication
python testrunner.py -i node.ini -c conf/security/py-jwt_auth_test.conf
```

### Failover Tests
```bash
# Auto-failover
python testrunner.py -i node.ini -c conf/failover/py-autofailover.conf

# Multi-node failover
python testrunner.py -i node.ini -c conf/failover/py-multinodefailover.conf

# Network partition failover
python testrunner.py -i node.ini -c conf/failover/py-autofailover-network-split.conf
```

### Rebalance Tests
```bash
# Rebalance in
python testrunner.py -i node.ini -c conf/rebalance/rebalance_in.conf

# Rebalance out
python testrunner.py -i node.ini -c conf/rebalance/rebalance_out.conf

# Swap rebalance
python testrunner.py -i node.ini -c conf/rebalance/swap_rebalance.conf
```

### Upgrade Tests
```bash
# KV upgrade
python testrunner.py -i node.ini -c conf/upgrade/kv_upgrade.conf

# CBAS upgrade
python testrunner.py -i node.ini -c conf/upgrade/cbas_upgrade.conf

# Offline upgrade
python testrunner.py -i node.ini -c conf/upgrade/offline_upgrade.conf
```

### Capella REST API Tests
```bash
# Cluster management
python testrunner.py -i node.ini -c conf/capella/cluster-v4-APIs.conf

# Bucket operations
python testrunner.py -i node.ini -c conf/capella/bucket-v4-APIs.conf

# Collections and scopes
python testrunner.py -i node.ini -c conf/capella/scope-v4-APIs.conf
```

### Serverless Tests
```bash
# Serverless sanity
python testrunner.py -i node.ini -c conf/serverless/sanity.conf

# Tenant management
python testrunner.py -i node.ini -c conf/serverless/tenant_mgmt.conf

# Metering and throttling
python testrunner.py -i node.ini -c conf/serverless/metering.conf
```

### Transaction Tests
```bash
# N1QL transactions
python testrunner.py -i node.ini -c conf/N1qlTransaction/basic.conf

# ACID atomicity
python testrunner.py -i node.ini -c conf/Atomicity/transaction_basic.conf

# Durability with transactions
python testrunner.py -i node.ini -c conf/N1qlTransaction/txn_durability_level.conf
```

### Backup and Restore Tests
```bash
# Simple backup
python testrunner.py -i node.ini -c conf/backup_restore/simple_test.conf

# Continuous backup
python testrunner.py -i node.ini -c conf/backup_restore/continuous_backup_test.conf
```

## Special Execution Modes

### Sirius Go Client (Alternative Document Loading)
```bash
# Launch Sirius process client with sirius_go_sdk parameter
python testrunner.py -i node.ini -c conf/sanity.conf -p load_docs_using=sirius_go_sdk --launch_sirius_process --sirius_url http://localhost:8080

# Launch Sirius Docker client with sirius_go_sdk parameter
python testrunner.py -i node.ini -c conf/sanity.conf -p load_docs_using=sirius_go_sdk --launch_sirius_docker --sirius_url http://localhost:8080
```

### Include/Exclude Tests
```bash
# Include specific tests
python testrunner.py -i node.ini -c conf/sanity.conf -d "epengine.*,security.*"

# Exclude specific tests
python testrunner.py -i node.ini -c conf/sanity.conf -e "cbas.*,upgrade.*"
```

### Global Test Search
```bash
# Search across all conf directories
python testrunner.py -i node.ini -g "*.conf"
```

## Runtype-Specific Execution

### On-Premise (default)
```bash
python testrunner.py -i node.ini -c conf/sanity.conf -p runtype=default
```

### Dedicated Clusters
```bash
python testrunner.py -i node.ini -c conf/capella/cluster-v4-APIs.conf -p runtype=dedicated
```

### Serverless
```bash
python testrunner.py -i node.ini -c conf/serverless/sanity.conf -p runtype=serverless
```

### Columnar
```bash
python testrunner.py -i node.ini -c conf/columnar/copy_to_kv.conf -p runtype=columnar
```

## Validation and Linting

**Current Status**: No linting, type checking, or formatting configured.

**Recommended** (not currently implemented):
```bash
# If pyproject.toml is configured with ruff/black/mypy
ruff check .
ruff format .
mypy .
```

## Coverage

**Current Status**: No coverage tracking configured.

**Recommended** (not currently implemented):
```bash
# If coverage tools are configured
python -m coverage run testrunner.py
coverage report --html
```
