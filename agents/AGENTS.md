# Agents & Skills

This directory contains all agent definitions and skills for TAF.
`.factory/droids/` and `.factory/skills/` contain `@`-reference files pointing into this directory — do not edit files there directly.

## Structure

```
agents/
  <name>.md     – feature-specific agent definitions
  skills/
    AGENTS.md   – skills index (this chain continues here)
    <name>.md   – individual skill definitions
```

## Available Agents

| Agent | File | Purpose |
|-------|------|---------|
| Fusion Test Architect | [fusion.md](fusion.md) | Writing fusion accelerator tests |
| Fusion Bug Triage | [fusion-triage.md](fusion-triage.md) | Debugging fusion issues, deciding AV vs MB, filing with correct Jira fields |

## Skills

See [skills/AGENTS.md](skills/AGENTS.md) for the full list of available skills and usage guidance.

## .claude/ — Harness Configuration

`.claude/` is the Claude Code harness config layer. It activates agents and skills defined in `agents/`.

| File | Purpose |
|---|---|
| `.claude/settings.json` | Registers skills (name, description, path into `agents/skills/`) and configures permissions/hooks. Committed — shared by all contributors. |
| `.claude/settings.local.json` | Local overrides (personal permissions, env vars). Not committed. |
| `.claude/skills/<name>/SKILL.md` | Per-skill entry point read by the harness — contains `@agents/skills/<name>.md` to forward to the canonical skill file. |
| `.claude/agents/<name>.md` | Per-agent entry point read by the harness — contains `@agents/<name>.md` to forward to the canonical agent file. Committed, same as `.factory/droids/<name>.md`. |

**Skills must be registered in `.claude/settings.json` to be invocable via `/skill-name`.** Adding a `.md` file under `agents/skills/` alone is not enough — see "Adding a new skill" in the root `AGENTS.md`.

## Adding a New Agent

1. Create `agents/<name>.md` with YAML frontmatter:
   ```yaml
   ---
   name: your-agent-name
   description: What this agent specializes in
   model: inherit
   ---
   ```
2. Create `.factory/droids/<name>.md` containing: `@agents/<name>.md`
3. Create `.claude/agents/<name>.md` containing: `@agents/<name>.md` — this is what makes the agent invokable as a Claude Code subagent
4. Add an entry to the table above
