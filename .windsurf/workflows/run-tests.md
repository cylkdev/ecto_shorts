---
description: Runs repository tests from the smallest useful scope to the widest relevant scope by using `.agent/PROJECT.md` for the exact commands, prerequisite repair, and widening order.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing behaviour, queries, changesets, public APIs, or tests and you need to prove the change with Elixir test runs.

## When not to use

Do **not** use this workflow to run Credo or Dialyzer.

## Source of truth

Use `.agent/PROJECT.md` as the source of truth for the repository root, the exact test commands, prerequisite repair, and validation breadth.

Use `Command Surface` for the test-command ladder. Use `Repair Failed Command Prerequisites` when the selected test command cannot reach ExUnit. Use `Recommended Validation Paths` when you need to confirm how wide the repository validation should become after the smallest test passes.

## What to do

1. Read `.agent/PROJECT.md`.

2. Confirm in `Repository Snapshot` that this repository runs tests from the repository root.

3. Choose the smallest useful test command from `Command Surface`.

4. If that test command fails before ExUnit reaches the real failing test, use `Repair Failed Command Prerequisites`, then rerun the same smallest command.

5. Once ExUnit reaches the real failing test, fix the failure.
   - Read the failing test name, assertion line, diff, or first useful project stacktrace line.
   - Decide whether the implementation is wrong or the test expectation is wrong before you edit anything.
   - If you conclude that the test expectation is wrong, or the intended behaviour is still unclear, stop and ask the user before changing the test.
   - Make the smallest change that resolves the failure.

6. Re-run the same smallest command until it passes with `0 failures`.

7. Widen from the smallest scope to the widest relevant scope by following the order in `.agent/PROJECT.md`.

8. Repeat the fix-and-rerun cycle until every required scope passes.

9. Stop only when the widest relevant test command from `.agent/PROJECT.md` passes with `0 failures`.

## Report back

State that you used the repository-root test-command ladder from `.agent/PROJECT.md`, identify the smallest and widest scopes you ran, and say whether the final run was clean.
