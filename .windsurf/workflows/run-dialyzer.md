---
description: Runs the repository Dialyzer workflow by using `.agent/PROJECT.md` for the exact command, the repository root, and prerequisite repair.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing Elixir code, types, specs, behaviours, or function return shapes and you need to verify static analysis with Dialyzer.

## When not to use

Do **not** use this workflow to run Credo or tests.

## Source of truth

Use `.agent/PROJECT.md` as the source of truth for the repository root, the canonical Dialyzer command, and prerequisite repair.

Use `Command Surface` for the exact Dialyzer command. Use `Repair Failed Command Prerequisites` when Dialyzer cannot reach a meaningful result. Use `Recommended Validation Paths` when you need to confirm that Dialyzer belongs in the chosen validation path.

## What to do

1. Read `.agent/PROJECT.md`.

2. Confirm in `Repository Snapshot` that this repository runs quality-check commands from the repository root.

3. Use the Dialyzer command from `Command Surface`.

4. If Dialyzer fails before it can analyze the code, use `Repair Failed Command Prerequisites` and rerun the same Dialyzer command.

5. Fix the reported warnings.
   - Start with the first warning in the output.
   - Prefer fixing the real mismatch between the code and its types, specs, or control flow.
   - Do not silence a warning unless the repository already uses an ignore file or the task explicitly allows that approach.
   - Re-run the same Dialyzer command after each coherent batch of fixes.

6. Handle existing warnings explicitly.
   - If the command already reports unrelated warnings that were present before your change, record that fact before making more edits.
   - Do not claim success unless the selected command is clean or the user explicitly accepts the remaining baseline.

7. Stop only when the repository-root Dialyzer command exits with code `0` and no unexpected warnings.

## Report back

State that you ran the repository-root Dialyzer command from `.agent/PROJECT.md` and say whether the final run was clean.
