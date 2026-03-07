---
name: run-checks
description: Chooses and runs the right verification checks after Elixir changes by using Credo only for rename-only refactors, or using Credo, Dialyzer, and tests when function inputs or outputs, types, specs, behaviours, or runtime behaviour changed.
---

## When to use

Use this skill when the current unresolved question is which verification path to use after changing Elixir code, tests, or project configuration.

## When not to use

Do **not** use this skill when the user explicitly asked for only Credo, only Dialyzer, or only tests. Use the matching skill instead.

Do **not** use this skill for documentation-only changes or other changes that did not affect Elixir code, tests, or project configuration.

## What to do

1. Read `.windsurf/workflows/run-checks.md`.

2. Treat `.windsurf/workflows/run-checks.md` as the source of truth.

3. Execute that workflow exactly as written.
