---
description: Runs the repository Credo workflow by using `.agent/PROJECT.md` for the exact command, the repository root, and prerequisite repair.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing Elixir code, tests, or project configuration and you need to verify linting and code-quality checks with Credo.

## When not to use

Do **not** use this workflow to run Dialyzer or tests.

## Source of truth

Use `.agent/PROJECT.md` as the source of truth for the repository root, the canonical Credo commands, and prerequisite repair.

Use `Command Surface` for the exact Credo command. Use `Repair Failed Command Prerequisites` when Credo cannot reach a meaningful lint result. Use `Recommended Validation Paths` when you need to confirm that Credo is the right validation path for the current change.

## What to do

1. Read `.agent/PROJECT.md`.

2. Confirm in `Repository Snapshot` that this repository runs quality-check commands from the repository root.

3. Choose the matching Credo command from `Command Surface`.
   - Use the strict local Credo pass for ordinary local iteration.
   - Use the CI-equivalent Credo command only when the task needs GitHub Actions parity.

4. If Credo fails before it can lint the code, use `Repair Failed Command Prerequisites` and rerun the same Credo command.

5. Fix the reported issues.
   - Start with the first issue in the output.
   - Make the smallest change that removes the issue.
   - Keep behaviour unchanged unless the task explicitly allows a behaviour change.
   - Re-run the same Credo command after each coherent batch of fixes.

6. Stop only when the chosen Credo command exits with code `0` and no Credo issues.

## Report back

State that you ran the repository-root Credo command from `.agent/PROJECT.md`, name which Credo variant you chose, and say whether the final run was clean.
