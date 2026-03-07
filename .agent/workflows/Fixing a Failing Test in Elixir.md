# Fixing a Failing Test in Elixir

This document describes the workflow for fixing one failing Elixir test in a way that a human or coding agent can reproduce, investigate, and validate from the current working tree alone. Treat the reader as a complete beginner to this repository: they have only the current working tree, this guide, and the InvestigationLog they create while they work. There is no memory of prior attempts and no external context.

## Purpose

Use this guide to fix one failing Elixir test in a way that a complete beginner can reproduce, investigate, and validate from the current working tree alone.

Use it together with `.agent/INVESTIGATION_LOGS.md`. That file defines how to investigate and document the problem. This guide defines the Elixir and Ecto test-specific mechanics you need in order to reach the real failing test, inspect the test boundary, and validate the result at the right breadth.

Apply it when a test fails because application behaviour, test expectations, setup, or more than one of those things must be investigated. Use it for assertion failures, unexpected exceptions, wrong return values, wrong query results, wrong changeset errors, regressions, and setup failures that must be repaired before ExUnit can reach the real failing test.

## When to Use This Guide

Use this guide after you can point to a failing test command and `mix` can start that command.

Once ExUnit reaches the real failing test, create an InvestigationLog and follow `.agent/INVESTIGATION_LOGS.md` alongside this guide.

Use `Repair Setup Failures First` when the command fails before ExUnit can run the target test.

## Definitions

### Core test terms

- `ExUnit` is Elixir's built-in test framework. Run it with `mix test`.
- A `changeset` is the Ecto data structure that stores proposed changes and validation errors.
- A `query` is the Ecto expression that describes which records to read.
- A `Repo` is the module that runs database reads and writes.
- A `stacktrace` is the list of function calls that led to the failure.

### Project and failure terms

- An `umbrella project` has one top-level `mix.exs` and multiple child apps under `apps/`.
- A `child application` is one app inside an umbrella project. It can have its own `mix.exs`, `lib/`, and `test/`.
- An `InvestigationLog` is the working document defined in `.agent/INVESTIGATION_LOGS.md`. Use that file as the source of truth for how the InvestigationLog is structured and maintained.
- An `example mapping` is a short list of concrete `Given`, `When`, `Then`, and `And` examples that define the expected behaviour before you edit code or tests.
- A `setup failure` is a failure caused by missing dependencies, compile errors, missing config, an unavailable database, or an unavailable required service.
- A `flaky failure` is a failure that does not happen every run or changes when test order, time, randomness, or external systems change.

## Before You Start

1. Confirm that `mix` can start the target test command.
2. Use `Repair Setup Failures First` if the command stops before ExUnit reports the target test failure.
3. Return to this guide after the same command reaches ExUnit and reports the test failure itself.

## Repair Setup Failures First

Use this section when the target command fails before ExUnit reports the test failure itself.

Keep rerunning the same target command while you repair setup. Stop setup repair only when that same command reaches ExUnit and prints the target failing test.

Write each setup command and each useful setup error into the InvestigationLog.

### 1. Start from the target command

Run the exact command you plan to use for the failing test.

Examples:

    mix test
    mix test test/my_file_test.exs
    mix test test/my_file_test.exs:42

Read the first error that stops the run. Fix that error before you move to the next one.

### 2. Repair dependency failures

Treat the failure as a dependency failure when output mentions unchecked dependencies, missing packages, missing Hex packages, or dependency compile failures.

Run these commands from the project root that owns the test:

    mix deps.get
    mix deps.compile

Then rerun the target test command.

### 3. Repair compile failures

Treat the failure as a compile failure when output starts with `** (CompileError)` or another compiler error before ExUnit starts the test.

Open the first project file named in the error output or stacktrace.

Fix one compile error at a time. Then rerun the same target test command.

Do not widen the command while you are still trying to reach ExUnit.

### 4. Repair config failures

Treat the failure as a config failure when output mentions a missing application env key, a missing environment variable, or a function such as `Application.fetch_env!/2` failing before the test runs.

Search the working tree for the missing key, module, or environment variable name.

Check these files first:

- `config/config.exs`
- `config/test.exs`
- `config/runtime.exs`
- `test/test_helper.exs`
- `mix.exs`

Use values that already exist elsewhere in the repository for the test environment. Do not invent new config values without repository evidence.

Then rerun the same target test command.

### 5. Repair database failures

Treat the failure as a database failure when output mentions connection refused, database does not exist, relation does not exist, pending migrations, sandbox ownership, or another database startup error before the target test runs.

Check `mix.exs` for test aliases first. If the repository already defines a test setup alias, run that alias.

If the project uses Ecto tasks and no test alias already covers setup, run the standard database setup tasks from the project root that owns the test:

    mix ecto.create
    mix ecto.migrate

Then rerun the same target test command.

If the project is an umbrella app, prefer the nearest owning `mix.exs` unless the umbrella root defines the setup alias that the child app relies on.

### 6. Repair required-service failures

Treat the failure as a required-service failure when output shows a missing cache, queue, HTTP dependency, containerized service, or another process the tests depend on before the target test runs.

Search the repository for the service name.

Check these files first:

- `README.md`
- `docker-compose.yml`
- `docker-compose.yaml`
- `compose.yml`
- `compose.yaml`
- `config/test.exs`
- `test/test_helper.exs`
- `test/support/`

Start the service the way the repository already documents or scripts it. If the repository provides a test fake, stub, bypass, or local helper instead of the real service, use that existing test path.

Then rerun the same target test command.

### 7. Stop and ask the user when setup is still undefined

Stop and ask the user when the repository does not show how to satisfy the missing dependency, config value, database, or required service without guessing.

Ask only after you searched the working tree for the failing key, module, service name, or task and still cannot find a supported setup path.

## Find the Project Root That Owns the Test

Follow one deterministic rule. Start from the failing test file path. Move upward until you find the nearest directory that contains `mix.exs`. Run the test command from that directory.

Use the umbrella root only when the failing file lives in the umbrella root `test/` directory.

If you only know the failing path from a CI log or failure report, use that path to choose the project root before you rerun anything.

Example: non-umbrella project

    my_app/
      mix.exs
      lib/
      test/
        user_test.exs

If the failing file is `my_app/test/user_test.exs`, run the command from `my_app/`.

Example: umbrella root test

    my_umbrella/
      mix.exs
      test/
        integration_test.exs
      apps/
        billing/
        accounts/

If the failing file is `my_umbrella/test/integration_test.exs`, run the command from `my_umbrella/`.

Example: umbrella child application test

    my_umbrella/
      mix.exs
      apps/
        billing/
          mix.exs
          lib/
          test/
            invoice_test.exs
        accounts/
          mix.exs
          lib/
          test/

If the failing file is `my_umbrella/apps/billing/test/invoice_test.exs`, run the command from `my_umbrella/apps/billing/`.

## Use an InvestigationLog

Once ExUnit reaches the real failing test, create an InvestigationLog and follow `.agent/INVESTIGATION_LOGS.md` to the letter.

Use `.agent/INVESTIGATION_LOGS.md` as the source of truth for the diagnosis process, the InvestigationLog structure, the communication rules, the restartability standard, the current-state history, and the decision record.

Use this guide as the source of truth for Elixir test mechanics. This guide owns project root selection, setup repair, test command choice, failure triage, test-specific inspection, and validation breadth.

Do not create a separate ad hoc note that duplicates the InvestigationLog. Record the investigation in the InvestigationLog itself.

## Record These Test-Specific Facts in the InvestigationLog

Use the InvestigationLog for the full investigation. Record these Elixir test-specific facts in it as you work.

### Reproduce

- [ ] Run the exact command that reproduces the original failure.
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

- [ ] Run the smallest test command that exercises the change.
- [ ] Record the smallest test command, its output, and its final `0 failures` result in the InvestigationLog.
- [ ] Run the whole test file after the smallest command passes.
- [ ] Run the widest required `mix test` command from the project root that owns the test.
- [ ] Record the wider commands, outputs, and final `0 failures` results in the InvestigationLog.
- [ ] Write one sentence that explains how the previous buggy behaviour would fail now, or record `No test logic changed.`

## Choose the Test Command

Run all commands from the project root that owns the failing test.

    mix test
    mix test test/my_file_test.exs
    mix test test/my_file_test.exs:42

Choose the test command in this order:

1. If you know the exact failing test line, run the line-targeted command first.

       mix test test/my_file_test.exs:42

2. If you know the failing test file but not the exact line, run the file.

       mix test test/my_file_test.exs

3. If you do not know which file is failing yet, run the wider test command that reveals the failure.

       mix test

4. After you find the failing test, stop using the wider command and switch to the smallest command that still reproduces the same failure.

5. Use `mix test --failed` only after you already ran tests in the same working copy and want to rerun the failures from that earlier run. Do not use it as the first command when you are still trying to locate the failure.

## Triage the Failure Before Editing

Read the first failing lines before you open any file for edits.

1. Treat output as a `setup failure` when it stops at dependency, compile, config, database, or required-service errors. Use `Repair Setup Failures First`. Then return to this guide.
2. Treat output as a `deterministic failure` when the same smallest command fails the same way every run. Continue with the workflow.
3. Treat output as a `flaky failure` when it changes with test order, current time, randomness, async execution, or external services. Stabilize the test before you change behaviour.
4. Treat output as `behaviour unclear` when nearby tests, callers, docs, and names do not define the intended result. Stop and ask the user after you collect concrete examples.

## Test-Specific Investigation Flow

Use the `Investigation Workflow` and `Communication Rules During Investigation` sections from `.agent/INVESTIGATION_LOGS.md` as the source of truth for the diagnosis process itself.

Use the steps below for the Elixir test-specific mechanics that surround that process.

### 1. Reproduce the failure with one exact test command

Choose the smallest test command that reproduces the failure and keep using that same command until the failure is understood.

Do not change code or tests until that same command can reproduce the same failure on demand.

Choose a command that reports exactly one failing test whenever possible.

### 2. Classify the failure

Write the failure class in the InvestigationLog. Use one of these values: `setup failure`, `deterministic failure`, `flaky failure`, or `behaviour unclear`.

Use the `Triage the Failure Before Editing` section to choose the next step.

### 3. Stabilize a flaky failure before you change behaviour

Make a flaky failure deterministic before you edit behaviour.

Remove dependence on current time, randomness, test order, async races, shared database state, or external service availability. Keep rerunning the smallest command until the same failure appears every time.

### 4. Read the test first, then follow the code path

Open the failing test and read it from top to bottom. Identify the setup, the call under test, and the assertion.

Read the test before you edit the implementation. Then follow that call into the implementation and read the code path in order.

If ExUnit prints `left` and `right`, do not guess what they mean. Read the assertion itself. `left` and `right` only refer to the two sides of the assertion as written.

### 5. Inspect the test boundary and the relevant collaborators

Track the data as it moves through arguments, maps, structs, pattern matching, function clauses, helper functions, and collaborator calls.

Inspect the test setup too. Check fixtures, factories, helpers, input values, return values, raised errors, messages, and process state that affect the failing behaviour.

If the test touches Ecto or the database, also inspect queries, changesets, preloads, transaction boundaries, `Repo` calls, and sandbox ownership.

If you stop understanding the data, add temporary `IO.inspect/2` or `dbg/2`, rerun the smallest test, and remove the debug output after you learn what changed.

### 6. Expand validation only after the smallest command passes

Run the whole test file when the smallest failing test passes.

Run the widest required `mix test` command from the project root that owns the failing test when the file passes.

Treat this step as required. Do not stop at a single passing targeted test if the change breaks other behaviour.

### 7. Confirm the test is still meaningful

Add or improve a test when the failure exposed a real bug.

Rewrite the assertion or the test name when you changed the test because the old expectation was wrong.

Do not keep a test that would pass no matter what the code does.

## Common Failure Patterns

Use these examples when the next step is still unclear.

- Compile error before ExUnit starts. Example: `** (CompileError)` or a missing dependency module. Use `Repair Setup Failures First`.
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

1. Rerun the smallest command that originally failed and confirm that it ends with `0 failures`.
2. Run the whole test file and confirm that it ends with `0 failures`.
3. Run the widest required `mix test` command from the project root that owns the failing test and confirm that it ends with `0 failures`.

If you changed a test, explain why the new test would fail if the old buggy behaviour returned.
