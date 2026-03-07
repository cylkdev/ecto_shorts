---
name: setup-elixir-project
description: Sets up a new Elixir project by following the checked-in scaffolding workflow and validating it with the repository command guide.
---

## When to use

Use this skill when the current unresolved question is how to set up a new Elixir project in this repository style.

## When not to use

Do **not** use this skill when the current unresolved question is one feature change, one failing test, or post-change verification for an existing project. Use the matching skill instead.

## Requirements

- Read `.agent/workflows/Create Elixir Project.md`. Treat it as the source of truth for scaffolding, configuration, and baseline project structure.
- Read `.agent/PROJECT.md`. Treat it as the source of truth for repository validation commands.

## What to do

1. Read `.agent/workflows/Create Elixir Project.md`.

2. Read `.agent/PROJECT.md`.

3. Follow `.agent/workflows/Create Elixir Project.md` for generator choice, required files, dependencies, Mix configuration, and cleanup of generated starter code.

4. Use `.agent/PROJECT.md`, especially `Command Surface` and `Recommended Validation Paths`, for the validation commands you run after the scaffold is in place.

5. Repeat until the scaffold and required validation checks pass or a real blocker appears.
