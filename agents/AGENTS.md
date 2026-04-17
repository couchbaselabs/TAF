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

## Skills

See [skills/AGENTS.md](skills/AGENTS.md) for the full list of available skills and usage guidance.

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
3. Add an entry to the table above
