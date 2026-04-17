# TAF Skills Index

This directory contains skills that TAF agents can invoke.

## Available Skills

### source-attribution
Adds source citations to responses indicating which documentation files provided the information.

**Usage:** Automatically applied when providing guidance about TAF programming patterns, repository structure, test execution, or architecture.

**Key features:**
- Cites documentation sources (AGENTS.md, docs/*, agents/*)
- Distinguishes TAF-specific from general knowledge
- Provides clear traceability for recommendations
- Helps verify accuracy of guidance

**See also:** `source-attribution/SKILL.md`

## Adding New Skills

To add a new skill:
1. Create a new directory under `.factory/skills/<skill-name>/`
2. Create `SKILL.md` with YAML frontmatter (name, description)
3. Follow the TAF documentation structure and conventions
4. Update this index file with skill description and usage
