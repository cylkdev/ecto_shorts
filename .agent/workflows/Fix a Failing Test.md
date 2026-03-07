# Fix a Failing Test

This document describes the workflow for fixing one failing Elixir test in a way that a human or coding agent can reproduce, investigate, and validate from the current working tree alone. Treat the reader as a complete beginner to this repository: they have only the current working tree, this guide, and the InvestigationLog they create while they work. There is no memory of prior attempts and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose

Use this guide to fix one failing Elixir test in a way that a complete beginner can reproduce, investigate, and validate from the current working tree alone.

Use it together with `.agent/INVESTIGATION_LOGS.md` and `.agent/PROJECT.md`. `.agent/INVESTIGATION_LOGS.md` defines how to investigate and document the problem. `.agent/PROJECT.md` defines the repository root, the exact test commands, the prerequisite-repair path, and the wider validation surface. This guide owns the failure-triage and investigation flow that surrounds those command mechanics.

Apply it when a test fails because application behaviour, test expectations, setup, or more than one of those things must be investigated. Use it for assertion failures, unexpected exceptions, wrong return values, wrong query results, wrong changeset errors, regressions, and setup failures that must be repaired before ExUnit can reach the real failing test.

## When to Use This Guide

Use this guide after you can point to one failing test command or one failing test report.

Read `.agent/PROJECT.md` first. Use its `Repository Snapshot`, `Command Surface`, `Repair Failed Command Prerequisites`, and `Recommended Validation Paths` sections as the source of truth for where commands run, how you choose the smallest test command, how you repair setup failures, and how wide final validation should become.

Once ExUnit reaches the real failing test, create an InvestigationLog and follow `.agent/INVESTIGATION_LOGS.md` alongside this guide.

## Before You Start

1. Read `.agent/PROJECT.md`.
2. Choose the smallest reproducing test command from `.agent/PROJECT.md` and let one coordinator own that command.
3. If that command fails before ExUnit reaches the real failing test, use `.agent/PROJECT.md` to repair prerequisites, then rerun the same command.
4. Record each setup command and each useful setup error in the InvestigationLog.
5. Return to this guide once the same test command reaches ExUnit and reports the test failure itself.

## Repair Setup Failures First

Use the `Repair Failed Command Prerequisites` section from `.agent/PROJECT.md` when the selected test command fails before ExUnit reports the target test failure.

Keep rerunning the same smallest command while you repair setup. Stop setup repair only when that same command reaches ExUnit and prints the target failing test.

Do not widen the command while you are still trying to reach the real failure. The goal of setup repair is not to pass more tests. The goal is to make one exact command reach the failure you actually need to understand.

## Use an InvestigationLog

Once ExUnit reaches the real failing test, create an InvestigationLog and follow `.agent/INVESTIGATION_LOGS.md` to the letter.

Use `.agent/INVESTIGATION_LOGS.md` as the source of truth for the diagnosis process, the InvestigationLog structure, the communication rules, the restartability standard, the current-state history, and the decision record.

Use this guide as the source of truth for the failure-triage flow that surrounds the investigation. Use `.agent/PROJECT.md` as the source of truth for the repository-root test command, setup repair, and wider validation path.

Do not create a separate ad hoc note that duplicates the InvestigationLog. Record the investigation in the InvestigationLog itself. Treat the InvestigationLog as the mailbox where worker-pass results are collected before the coordinator decides whether the implementation, the test expectation, or the behaviour definition is wrong.

## Record These Test-Specific Facts in the InvestigationLog

Use the InvestigationLog for the full investigation. Record these Elixir test-specific facts in it as you work.

### Reproduce

- [ ] Run the smallest reproducing test command chosen from `.agent/PROJECT.md`.
- [ ] Record that command in the InvestigationLog.
- [ ] Record the output of that command in the InvestigationLog.
- [ ] Record the failing test name in the InvestigationLog.
- [ ] Record the final failure count in the InvestigationLog.

### Understand

- [ ] Write one or two sentences that describe the setup, the action, and the expected result.
- [ ] Record the exact assertion line, diff line, or first useful project stacktrace line that shows the mismatch.
- [ ] Record the failure classification in the InvestigationLog.

### Decide

- [ ] Choose exactly one decision: `current implementation is wrong`, `current expectation is wrong`, or `intended behaviour is still unclear`.
- [ ] Record that decision in the InvestigationLog exactly as written.
- [ ] Record at least one concrete piece of evidence that supports that decision.
- [ ] Ask the user to sign off if the decision is `current expectation is wrong`, and record the user's answer in the InvestigationLog.
- [ ] Record the file path and function name you changed, or record the exact test block you changed.

### Validate

- [ ] Re-run the same smallest test command until it ends with `0 failures`.
- [ ] Record the smallest passing command, its output, and its final `0 failures` result in the InvestigationLog.
- [ ] Widen validation by following `.agent/PROJECT.md`.
- [ ] Run the wider repository checks required by `.agent/PROJECT.md`.
- [ ] Record the wider commands, outputs, and final success results in the InvestigationLog.
- [ ] Write one sentence that explains how the previous buggy behaviour would fail now, or record `No test logic changed.`

## Triage the Failure Before Editing

Read the first failing lines before you open any file for edits.

1. Treat output as a `setup failure` when it stops at dependency, compile, config, database, or required-service errors. Use `Repair Setup Failures First`. Then return to this guide.
2. Treat output as a `deterministic failure` when the same smallest command fails the same way every run. Continue with the workflow.
3. Treat output as a `flaky failure` when it changes with test order, current time, randomness, async execution, or external services. Stabilize the test before you change behaviour.
4. Treat output as `behaviour unclear` when nearby tests, callers, docs, and names do not define the intended result. Stop and ask the user after you collect concrete examples.

## Test-Specific Investigation Flow

Use the `Investigation Workflow` and `Communication Rules During Investigation` sections from `.agent/INVESTIGATION_LOGS.md` as the source of truth for the diagnosis process itself.

Use the steps below for the test-specific mechanics that surround that process.

### 1. Reproduce the failure with one exact test command

Choose the smallest test command from `.agent/PROJECT.md` that reproduces the failure and keep using that same command until the failure is understood.

Do not change code or tests until that same command can reproduce the same failure on demand.

Choose a command that reports exactly one failing test whenever possible.

### 2. Classify the failure

Write the failure class in the InvestigationLog. Use one of these values: `setup failure`, `deterministic failure`, `flaky failure`, or `behaviour unclear`.

Use the `Triage the Failure Before Editing` section to choose the next step.

### 3. Let one coordinator own the failing command

One coordinator owns the smallest reproducing command, the current failure class, and the next bounded question. Do not let multiple in-flight guesses compete over whether the next move is setup repair, expectation review, or behaviour clarification.

Keep the same smallest reproducing command until the failure class changes or the coordinator explicitly widens validation.

### 4. Fan out bounded worker passes only after the failure class is stable

After the smallest command and the failure class are stable, the coordinator may fan out bounded worker passes for independent evidence such as one helper path, one caller, one setup file, one stacktrace branch, or one query path.

Record each worker-pass result in the InvestigationLog sections that fit it, such as `Facts`, `Progress`, `Surprises & Discoveries`, `Decision Log`, or `Open Questions / Blockers`. Collect those results before you change code, rewrite the test, or widen the command.

### 5. Stabilize a flaky failure before you change behaviour

Make a flaky failure deterministic before you edit behaviour.

Remove dependence on current time, randomness, test order, async races, shared database state, or external service availability. Keep rerunning the smallest command until the same failure appears every time.

### 6. Read the test first, then follow the code path

Open the failing test and read it from top to bottom. Identify the setup, the call under test, and the assertion.

Read the test before you edit the implementation. Then follow that call into the implementation and read the code path in order.

If ExUnit prints `left` and `right`, do not guess what they mean. Read the assertion itself. `left` and `right` only refer to the two sides of the assertion as written.

### 7. Inspect the test boundary and the relevant collaborators

Track the data as it moves through arguments, maps, structs, pattern matching, function clauses, helper functions, and collaborator calls.

Inspect the test setup too. Check fixtures, factories, helpers, input values, return values, raised errors, messages, and process state that affect the failing behaviour.

If the test touches Ecto or the database, also inspect queries, changesets, preloads, transaction boundaries, `Repo` calls, and sandbox ownership.

If you stop understanding the data, add temporary `IO.inspect/2` or `dbg/2`, rerun the smallest test, and remove the debug output after you learn what changed.

### 8. Expand validation only after the smallest command passes

Run the smallest failing test command until it passes, then widen validation by following `.agent/PROJECT.md`.

Treat this step as required. Do not stop at a single passing targeted test if the change breaks other behaviour.

### 9. Confirm the test is still meaningful

Add or improve a test when the failure exposed a real bug.

Rewrite the assertion or the test name when you changed the test because the old expectation was wrong.

Do not keep a test that would pass no matter what the code does.

## Common Failure Patterns

Use these examples when the next step is still unclear.

- Compile error before ExUnit starts. Use `Repair Setup Failures First` and `.agent/PROJECT.md`.
- Sandbox or shared-state failure. Example: the test passes alone but fails in the suite, or the output shows ownership or leftover data problems. Stabilize isolation before you change behaviour.
- Stale expectation. Example: nearby callers and docs use one return shape, but an old test expects another. Collect evidence before you edit the test.
- Caller-dependent behaviour. Example: several callers and nearby passing tests rely on the current public behaviour. Fix the implementation only after you confirm that shared contract.

## Use Concrete Examples When Behaviour Is Unclear

If the repository does not define the expected behaviour clearly enough to choose a decision without guessing, stop editing and use the concrete-example step from `.agent/INVESTIGATION_LOGS.md`.

Write those examples in the InvestigationLog itself.

If the ambiguity is larger than one failing test or touches multiple rules, move to `.agent/EXAMPLE_MAPPING.md`.

## Test-Specific Decision Criteria

Change the implementation only when the failing test matches the real expected behaviour and the current implementation breaks that behaviour.

Change the test only when nearby callers, passing tests, doc comments, or clear module and function names show that the current implementation already matches the intended behaviour, the failing test expects something outdated or incorrect, and the user signs off on changing the test.

Do not change tests or code when the repository does not yet define the behaviour clearly enough to edit without guessing. Use the concrete-example step from `.agent/INVESTIGATION_LOGS.md` to resolve the expected behaviour first.

## Validation and Acceptance

Record all validation in the InvestigationLog.

1. Re-run the smallest command that originally failed and confirm that it ends in the expected success state.
2. Widen validation by following `.agent/PROJECT.md` and confirm that each required wider scope ends in the expected success state.
3. Use `.agent/PROJECT.md` to choose and run the wider repository checks required from the repository root.

If you changed a test, explain why the new test would fail if the old buggy behaviour returned.
