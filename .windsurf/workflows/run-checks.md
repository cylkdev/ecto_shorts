---
description: Chooses the verification path after Elixir changes by using `.agent/PROJECT.md` and the specialized workflows to run the required checks.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing Elixir code, tests, or project configuration when you need to choose the right verification path.

## When not to use

Do **not** use this workflow when the user explicitly asked for only Credo, only Dialyzer, or only tests. Use the matching workflow directly.

Do **not** use this workflow for documentation-only changes or other changes that did not affect Elixir code, tests, or project configuration.

## Source of truth

Use `.agent/PROJECT.md`, especially `Recommended Validation Paths`, to choose the validation path.

Use `.windsurf/workflows/run-credo.md`, `.windsurf/workflows/run-dialyzer.md`, and `.windsurf/workflows/run-tests.md` to execute the chosen path.

## What to do

1. Classify the change. Use the `rename-only` path only when the change only renamed modules, functions, variables, files, aliases, or test names and did not change logic, runtime behaviour, function inputs or outputs, types, specs, callbacks, or test meaning. Use the broader path when the change affects function shape, types, specs, callbacks, behaviours, structs, queries, changesets, runtime behaviour, or test expectations because behaviour changed. If you are not sure, use the broader path.

2. Read `.agent/PROJECT.md` and confirm the matching path in `Recommended Validation Paths`.

3. For a `rename-only` change, read `.windsurf/workflows/run-credo.md` and execute it exactly as written. Stop when the final run is clean.

4. For a function-shape, type, or behaviour change, read and execute `.windsurf/workflows/run-credo.md`, `.windsurf/workflows/run-dialyzer.md`, and `.windsurf/workflows/run-tests.md` in that order.

5. If the task also needs GitHub Actions parity, use `CI-Equivalent Lint and Coverage Pass` in `.agent/PROJECT.md` after the chosen path is clean.

## Report back

State which classification you chose and which workflows or additional PROJECT paths you ran. Say that you ran only Credo for a `rename-only` change, or that you ran Credo, Dialyzer, and tests for the broader path.
