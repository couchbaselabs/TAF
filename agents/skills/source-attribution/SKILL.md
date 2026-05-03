---
name: source-attribution
description: >-
  Adds source citations to responses indicating which documentation files
  provided the information, or indicates if response is from general context.
  Use this whenever providing guidance to ensure transparency and traceability.
---

# Source Attribution

## Purpose

Ensure all agent responses transparently cite the source of information
being provided, distinguishing between documentation, skills, and general knowledge.

## When to Use

Apply this automatically when:
- Providing guidance about coding patterns
- Recommending specific implementations
- Explaining framework behavior
- Reference architecture or workflows
- Quoting conventions or standards

## Documentation Sources in TAF

**Framework Documentation:**
- `AGENTS.md` - Framework purpose, commands, repository layout, development patterns
- `README.md` - Setup instructions, environment requirements, execution commands
- `docs/architecture.humans.md` - Component architecture, module boundaries, runtime flows
- `docs/architecture.agents.md` - Agent-optimized architecture with data contracts and validation clues
- `docs/agent-context/repo-inventory.md` - Detailed component breakdown
- `docs/agent-context/build-test-matrix.md` - Execution commands by component
- `docs/agent-context/domain-glossary.md` - Couchbase and TAF terminology
- `docs/agent-context/troubleshooting.md` - Common issues and solutions
- `docs/agent-context/data-service/AGENTS.md` - KV/Data Service specific guidance
- `docs/agent-context/ns-server/AGENTS.md` - NS Server/Cluster Manager specific guidance

## Citation Format

**For documentation sources:**
```
[from <file-path>]
[from <file-path>, section: <section-name>]
[from AGENTS.md, section: Repository Layout]
[from docs/architecture.agents.md, section: Test Execution Layer]
```

**For skills:**
```
[from agents/<skill-name>/SKILL.md]
[from .factory/skills/<skill-name>/SKILL.md]
```

**For general knowledge:**
```
[from general context]
[from general Python best practices]
[from standard testing frameworks patterns]
[from generic common knowledge]
```

## Examples

**Example 1: Repository Structure**
```
To add a new test for bucket operations:

1. Create test file in pytests/bucket_collections/
2. Inherit from OnPremBaseTest (from AGENTS.md, section: Test Execution Model)
3. Use TestInputSingleton.input.param() for parameters (from AGENTS.md, section: Configuration Files)
4. Follow PEP8 standards (from general context)
```

**Example 2: Test Execution**
```
Run tests using:
python testrunner.py -i node.ini -c conf/sanity.conf (from AGENTS.md, section: Quick Commands)

For individual tests:
python testrunner.py -i node.ini -t epengine.basic_ops.basic_ops.test_doc_size (from AGENTS.md)
```

**Example 3: Architecture Guidance**
```
The testrunner.py initializes the cluster via:
1. Loads cluster configuration from node.ini (from docs/architecture.agents.md, section: Runtime Flow Diagram)
2. Selects base test class based on runtype parameter (from AGENTS.md, section: Test Execution Model)
3. Executes tests with parameters from .conf files (from general context)
```

## Rules

- **Always cite** when referencing specific framework behavior, patterns, or conventions
- **Cite the most specific source** - if info is in both AGENTS.md and docs/architecture.agents.md, cite the more detailed one
- **Use section citations** when documentation is long to help users verify
- **Distinguish TAF-specific** from general programming knowledge
- **Be honest** if citing general context - don't falsely attribute to documentation
- **Cite multiple sources** when relevant, separated by semicolons

## Detection Heuristics

**Cite documentation when:**
- Explaining TAF-specific directory structure or file locations
- Describing test execution patterns or frameworks
- Reference build commands or test commands
- Discussing component interactions or architecture
- Explaining configuration file formats
- Mentioning repository-specific conventions

**Use "general context" when:**
- Providing Python best practices (PEP8, etc.)
- Explaining standard testing patterns
- Discussing general software engineering principles
- Providing vanilla Python code examples
- Giving generic debugging advice not specific to TAF

## Response Pattern

When responding, automatically include citations inline or at the end:

```
Your guidance here (from AGENTS.md, section: X)

Multiple sentences here, each with source attribution (from docs/architecture.agents.md)
More details (from general context)
```

## Validation

After providing a response, verify:
- [ ] All TAF-specific information has a source citation
- [ ] General context is clearly marked as such
- [ ] Citations point to actual documentation that exists
- [ ] Section names are accurate (if using section citations)
