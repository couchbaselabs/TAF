# Skills Index

↑ [agents/AGENTS.md](../AGENTS.md) → root [AGENTS.md](../../AGENTS.md)

All skills live in `agents/skills/`. Each skill has a `.md` file with YAML frontmatter and is registered in `.claude/settings.json`.

`.factory/skills/` contains `@`-reference files pointing into this directory — do not edit files there directly.

## Available Skills

### source-attribution
Adds source citations to responses indicating which TAF documentation files provided the information.

**Usage:** Automatically applied when providing guidance about TAF programming patterns, repository structure, test execution, or architecture.

**Key features:**
- Cites documentation sources (`AGENTS.md`, `docs/*`, `agents/*`)
- Distinguishes TAF-specific from general knowledge
- Provides clear traceability for recommendations

**File:** `agents/skills/source-attribution.md`

---

## Adding a New Skill

1. Create `agents/skills/<name>.md` with YAML frontmatter:
   ```yaml
   ---
   name: your-skill-name
   description: What this skill does
   ---
   ```
2. Register in `.claude/settings.json` under `skills`
3. Create `.factory/skills/<name>/SKILL.md` containing: `@agents/skills/<name>.md`
4. Add an entry to this index file
