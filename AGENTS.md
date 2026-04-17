# TAF (Test Automation Framework)

## Purpose
TAF is the primary test automation framework for Couchbase Server and Capella.
It validates KV, NS Server, Magma storage, Fusion storage, Columnar analytics, and cross-component functionality across on-premise, dedicated, and serverless environments.

---

## Environment Requirements

- Python 3.10+ (recommended 3.10.14)
- Couchbase Server cluster (or Capella access)
- SSH access to cluster nodes (for on-premise)
- Git submodules initialized

---

## Quick Commands

### Setup
```bash
# Initialize submodules
git submodule init
git submodule update --init --force --remote

# Install dependencies (Python 3.10.14 required)
python -m pip install -r requirements.txt
```

### Development Setup
For contributors and developers working on TAF codebase:

```bash
# Install development dependencies (linting, type checking, hooks)
python -m pip install -r requirements-dev.txt

# Install pre-commit hooks (one-time setup after clone)
pre-commit install

# Optional: Install npm tools for additional checks
npm install -g jscpd  # Duplicate code detection
```

**Pre-commit hooks run automatically on commit:**
- Unit tests (blocks commit on failure)
- Ruff linter (auto-fixes issues)
- Ruff formatter (checks formatting)
- Mypy type checker (informational)
- Large file detection (1MB limit)
- Merge conflict detection
- AGENTS.md validation

**Manual checks:**
```bash
# Run all hooks manually
pre-commit run --all-files

# Run specific hook
pre-commit run ruff --all-files
pre-commit run mypy --all-files
pre-commit run unit-test --all-files
```

### Test Execution
```bash
# Run from test suite configuration
python testrunner.py -i node.ini -c conf/sanity.conf -p get-cbcollect-info=True

# Run individual test
python testrunner.py -i node.ini -t epengine.basic_ops.basic_ops.test_doc_size,nodes_init=1
```

### Document Loading
The framework supports multiple document loading options for test data generation.

**Document Loading Options:**
- `load_docs_using=default_loader` – Uses built-in Python SDK loader (default value)
- `load_docs_using=sirius_java_sdk` – Uses Sirius Java SDK for document operations via DocLoader
- `load_docs_using=sirius_go_sdk` – Uses Sirius Go SDK for document operations via sirius submodule

**Launch DocLoader within test:**
```bash
# Start Java-based REST doc loader and use sirius_java_sdk for document loading
python testrunner.py -c conf/sanity.conf -i node.ini -p rerun=False,get-cbcollect-info=False,skip_cluster_reset=True,load_docs_using=sirius_java_sdk --launch_java_doc_loader --sirius_url http://localhost:<port_num>
```

**Key parameters:**
- `load_docs_using=default_loader` – Use built-in Python SDK (default, requires no additional flags)
- `load_docs_using=sirius_java_sdk` – Use Sirius Java SDK for document operations (requires `--launch_java_doc_loader`)
- `load_docs_using=sirius_go_sdk` – Use Sirius Go SDK for document operations via sirius submodule
- `--launch_java_doc_loader` – Flag to launch Java DocLoader process within test execution
- `--launch_sirius_process` – Flag to launch Sirius Go client process within test execution
- `--launch_sirius_docker` – Flag to launch Sirius Go client in Docker within test execution
- `--sirius_url <url>` – DocLoader or Sirius endpoint URL (e.g., http://localhost:8080)

**Manual DocLoader execution (separate process):**
```bash
# Start DocLoader standalone (in DocLoader directory)
cd DocLoader
mvn install
java -cp ./target/magmadocloader/magmadocloader.jar RestServer.RestApplication --server.port=<port_num>

# Then run TAF without --launch_java_doc_loader
python testrunner.py -c conf/sanity.conf -i node.ini -p load_docs_using=sirius_java_sdk
```

---

## Repository Layout

### Core Entry Points
- `testrunner.py` - Main test runner with unittest framework and command-line parsing
- `pytests/basetestcase.py` - Base test case factory that selects appropriate base class

### Test Execution Model
The `runtype` parameter determines test environment:
- `default`: Uses `OnPremBaseTest` – on-premise Couchbase Server
- `columnar`: Uses `ColumnarBaseTest` – Columnar analytics service
- `dedicated`: Uses `ProvisionedBaseTestCase` – dedicated cloud clusters
- `serverless`: Uses `OnCloudBaseTest` – Capella serverless

### Key Directories

`pytests/` – Test implementation (all test code must go here)
- `basetestcase.py` – Base test class selector based on runtype
- Component directories: `epengine/`, `cbas/`, `security/`, `storage/`, `upgrade/`, etc.

`lib/` – Core framework libraries
- `sdk_client3.py` – Python SDK client wrapper
- `couchbase_helper/` – Cluster operations, document generators
- `BucketLib/` – Bucket operations via REST API
- `CbasLib/` – Columnar/Analytics operations
- `framework_lib/` – Test runner utilities and command-line parser
- `Jython_tasks/` – Jython task execution framework
- `SystemEventLogLib/` – System event log validation

`couchbase_utils/` – Feature-specific utilities
- `cb_server_rest_util/` – Direct Couchbase REST API mappings
- `security_utils/` – Security operations (TLS, encryption, certificates)
- `bucket_utils/` – Bucket management helpers
- `upgrade_utils/`, `rebalance_utils/`, etc.

`platform_utils/` – Infrastructure utilities
- `ssh_util/` – Paramiko-based SSH session management
- `error_simulation/` – Network and system error simulation

`conf/` – Test suite configurations
- `<component>/test.conf` – Test selections and parameters
- `node.ini` – Cluster topology and credentials

`py_constants/` – Test constants
- `cb_constants/CBServer.py` – Server version mappings
- `cb_constants/system_event_log.py` – Event log schemas

---

## Development Patterns

### Naming Conventions
TAF follows PEP8 naming conventions enforced by ruff:

- **Functions/Methods**: `snake_case` (e.g., `test_document_crud`, `create_bucket`)
- **Variables**: `snake_case` (e.g., `bucket_name`, `doc_count`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAX_RETRIES`, `DEFAULT_TIMEOUT`)
- **Classes**: `PascalCase` (e.g., `OnPremBaseTest`, `BucketUtils`)
- **Module names**: `snake_case` (e.g., `bucket_utils.py`, `cluster_ready_functions.py`)
- **Private members**: `_leading_underscore` (e.g., `_internal_method`)
- **Test methods**: `test_` prefix with `snake_case` (e.g., `test_rebalance_after_failover`)

**Exceptions for Unittest compatibility:**
- `setUp`, `tearDown`, `setUpClass`, `tearDownClass` are allowed
- MixedCase variables in existing code are tolerated but new code should use snake_case

### Adding Tests
1. Tests must live in `pytests/` directory
2. Inherit from appropriate base class based on component
3. Use `TestInputSingleton.input.param()` to access parameters
4. Follow PEP8 standards (see `agents/test-agent.md`)

### Configuration Files
- `.ini` files define cluster topology and credentials
- `.conf` files list test modules with parameters
  Format: `module.class.test_method,param1=value1,param2=value2`

### Parameter Passing
```python
# From .ini or command line
TestInputSingleton.input.test_params['get-cbcollect-info'] = True

# In test code
from TestInput import TestInputSingleton
param_value = TestInputSingleton.input.param("param_name", default_value)
```

### Common Utilities
```python
from sdk_client3 import SDKClient  # Python SDK operations
from couchbase_utils(cb_server_rest_util) import *  # REST API calls
from couchbase_utils(bucket_utils) import *  # Bucket helpers
```

---

## Validation Requirements

Before completion, ensure:
1. Tests follow existing patterns in component directories
2. No hard-coded credentials or secrets
3. Proper cleanup in tearDown methods
4. Runtype parameter is respected
5. Test failures analyzed with root cause explanation

---

## Hard Constraints

- Do NOT modify git submodules:
  - `DocLoader/` – Java-based document generator (maintained separately)
  - `lib/capellaAPI/` – Capella REST API libraries (maintained separately)
  - `sirius/` – Go-based document client framework (maintained separately)
- All test code belongs in `pytests/` directories
- Never hard-code cloud identities or API keys
- Test failures must include detailed analysis

---

## Agents & Skills

### Structure
`agents/` is the canonical location for all agent definitions and skills:
```
agents/
  <name>.md  – feature-specific agents live directly here
  skills/    – general-purpose utility skills (e.g., source-attribution)
```

`.factory/droids/` and `.factory/skills/` contain symlinks into `agents/` — do not edit files there directly.

### Adding a new agent
1. Create `agents/<name>.md` with YAML frontmatter:
   ```yaml
   ---
   name: your-agent-name
   description: What this agent specializes in
   model: inherit
   ---
   ```
2. Create symlink: `ln -s ../../agents/<name>.md .factory/droids/<name>.md`

### Adding a new skill
1. Create `agents/skills/<name>.md` with YAML frontmatter:
   ```yaml
   ---
   name: your-skill-name
   description: What this skill does
   ---
   ```
2. Register in `.claude/settings.json` under `skills`
3. Create symlink: `ln -s ../../../agents/skills/<name>.md .factory/skills/<name>/SKILL.md`

### Available agents
- [Fusion Test Architect](agents/fusion.md) – writing fusion accelerator tests

### Available skills
- [Source Attribution](agents/skills/source-attribution.md) – cites TAF docs in responses

---

## Supporting Documentation
### Framework Documentation
- [Repo Inventory](docs/agent-context/repo-inventory.md) – Detailed component breakdown
- [Build Test Matrix](docs/agent-context/build-test-matrix.md) – Execution commands by component
- [Domain Glossary](docs/agent-context/domain-glossary.md) – Couchbase and TAF terminology
- [Troubleshooting Guide](docs/agent-context/troubleshooting.md) – Common issues and solutions
- [Test-Agent Skill](agents/test-agent.md) – Test writing guidance and constraints

### Component specific Documentation
- If asked about **KV** or **data-service**: refer to docs/agent-context/data-service/AGENTS.md
- If asked about **NS Server** or **Cluster-manager**: refer to docs/agent-context/ns-server/AGENTS.md
