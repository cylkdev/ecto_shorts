# Project Command Guide

This file is the repository-level command guide for EctoShorts. Treat the reader as a complete beginner to this repository: they have only the current working tree and this file. There is no memory of prior setup and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use this guide when the unresolved question is which repository command to run, where to run it, how to repair prerequisites when it fails, or how wide validation should be.

This guide is the only source of truth for repository-specific formatting, linting, static-analysis, coverage, and test commands. Other guides may decide when a check is required, but they should point back here for the exact command, the repository root, the prerequisite repair path, and the widening order.

## How to Use PROJECT.md

When you are about to run repository quality checks, use this guide first.

When you need project context, read `Repository Snapshot` first. When this is your first run in the working tree or database-backed commands fail before they reach the real work, use `First-Time Setup`. When you need exact commands, use `Command Surface`. When a command fails before it can produce a meaningful result, use `Repair Failed Command Prerequisites`. When you need to choose which checks fit the change you made, use `Recommended Validation Paths`.

If another guide tells you to "run the wider project checks" or "use the repository command surface," that instruction means to come back to this file, pick the matching path, and record the exact commands you chose from here.

## Coordination Rules

When you use this file to choose repository commands, copy the exact commands, working directory, and prerequisite or widening decisions into the surrounding active document's `Task and Key Files` section.

Keep the related document maintenance task visible in the surrounding active document's `Progress` section or checklist so command-surface updates are not treated as implicit.

If command-selection work changes another guide, workflow, or active document, update that companion document in the same change.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Repository Snapshot

EctoShorts is one root Mix project. Run repository commands from the repository root, which is the directory that contains this working tree's `mix.exs`. There is no umbrella layout and no child-application command routing to figure out.

The checked-in toolchain targets Elixir `~> 1.15` in `mix.exs`. The current working tree also pins `elixir 1.15.2-otp-25` and `erlang 25.3.2` in `.tool-versions`.

The repository keeps test configuration in `config/config.exs`. It does not have a separate `config/test.exs`. Test runs boot the database stack from `test/test_helper.exs`, which starts `:postgrex` and `EctoShorts.Repo`. Database-backed tests use the SQL sandbox in `test/support/data_case.ex`.

Migrations live under `priv/repo/migrations`. Coverage configuration lives in `coveralls.json`, which currently enforces a `90` percent minimum coverage threshold.

The checked-in database config expects PostgreSQL on `localhost` with the `postgres` user and the test database name `ecto_shorts_test`. If your local PostgreSQL auth requires extra setup, fix that locally before you trust test, coverage, or migration failures.

## First-Time Setup

When you run first-time setup, start from the repository root.

Match the checked-in toolchain before you do anything else. If you use `asdf`, the versions come directly from `.tool-versions`. If you use another toolchain manager, match Elixir `1.15.2` on OTP `25`.

Fetch dependencies:

    mix deps.get

Make sure PostgreSQL is running locally and accepts connections for the `postgres` user on `localhost`.

Prepare the test database explicitly:

    MIX_ENV=test mix ecto.setup

Use that command, not plain `mix setup`, when your goal is to prepare the test environment. In this repository, plain `mix setup` runs in the default environment you invoked it in. That is useful for default-environment setup, but it is not the safe shorthand for test-database preparation.

Once those steps succeed, you are ready to run the command surface below.

## Command Surface

Run every command in this section from the repository root.

### Formatting

When you changed Elixir source or test files and need to confirm formatting, run:

    mix format --check-formatted

### Linting with Credo

When you want the strict repository lint pass during local iteration, run:

    mix credo --strict

When you want the same Credo environment that the GitHub Actions workflow uses, run:

    CI=true MIX_ENV=test mix credo

### Static Analysis with Dialyzer

When types, specs, callbacks, struct shapes, function return shapes, or behaviour contracts changed, run:

    mix dialyzer

`mix.exs` already routes `dialyzer` through `preferred_cli_env`, so this command already runs in the repository's test-oriented environment without you needing to add `MIX_ENV=test`.

### Testing with ExUnit

When you run ExUnit, start from the smallest useful command and widen only after the smaller scope passes.

If you know the exact failing line, use:

    mix test path/to/file_test.exs:LINE

If you know the file but not the line, use:

    mix test path/to/file_test.exs

If you need the whole suite for this repository, use:

    mix test

Use `mix test --failed` only after you already ran a failing test command in the same working copy and want to rerun those recorded failures. Do not use it as your first discovery step.

### Coverage and Statistics with ExCoveralls

When you need the repository's coverage and statistics pass, or when you want to match the GitHub Actions coverage job locally, run:

    CI=true MIX_ENV=test mix coveralls.json

The checked-in `coveralls.json` requires at least `90` percent coverage.

## Repair Failed Command Prerequisites

Use this section when a command fails before it can produce the real lint, static-analysis, coverage, or test result you were trying to inspect.

### Dependencies are missing

If Mix reports missing packages, unchecked dependencies, or dependency compile failures, fetch and compile dependencies first:

    mix deps.get
    mix deps.compile

Then rerun the exact command that failed.

### The project does not compile

If the command fails with a compile error before the target check starts, fix the first project file named in the error output or stacktrace. Do not switch commands yet. Rerun the same command after each compile fix until the requested tool starts cleanly.

### PostgreSQL is not running

If test, coverage, or migration commands fail with connection errors, start PostgreSQL locally and confirm it accepts the configured `postgres@localhost` connection. In this repository, database-backed tests start the repo from `test/test_helper.exs`, so `mix test` and `mix coveralls.json` cannot succeed while PostgreSQL is unavailable.

Once PostgreSQL is running, rerun the same command.

### The test database does not exist yet

If PostgreSQL is running but the test database is missing, create and migrate it:

    MIX_ENV=test mix ecto.setup

Then rerun the exact command that failed.

### The test database schema is stale

If a test or coverage run reaches PostgreSQL but fails with schema drift errors such as `undefined_column`, your local test database is behind the checked-in migrations. In this repository, a concrete example is a failure like `column "permalink" of relation "posts" does not exist` or `column p0.custom_string_field does not exist`.

Reset the test database completely:

    MIX_ENV=test mix ecto.reset

Then rerun the same test or coverage command.

## Recommended Validation Paths

Use these paths when you need to decide which repository checks fit the change you made.

### Documentation-only changes

If you changed only Markdown, text, or other non-Elixir repository guidance, no Mix quality-check command is required unless the task explicitly asks for one. Validate the documentation change directly instead.

### Rename-only Elixir changes

Use this path when the change only renamed modules, functions, variables, files, aliases, or tests and did not change control flow, runtime behaviour, function inputs, function outputs, return shapes, types, specs, callbacks, or test meaning.

Run formatting if you edited `.ex` or `.exs` files, then run the strict local lint pass:

    mix format --check-formatted
    mix credo --strict

### Behaviour, Type, or Function-Shape Changes

Use this path when the change affects runtime behaviour, query results, changeset rules, public return shapes, function inputs, specs, callbacks, behaviours, structs, or anything else that Dialyzer or ExUnit should verify.

Run formatting if you edited `.ex` or `.exs` files. Then run the repository checks in this order:

    mix format --check-formatted
    mix credo --strict
    mix dialyzer

After those pass, run tests from the smallest useful scope to the widest relevant scope by using the commands from `Testing with ExUnit`.

### CI-Equivalent Lint and Coverage Pass

Use this path when you need to match the current GitHub Actions checks locally.

Run:

    CI=true MIX_ENV=test mix credo
    mix dialyzer
    CI=true MIX_ENV=test mix coveralls.json

If the task also needs proof of a focused or repository-wide test run, pair this path with the commands from `Testing with ExUnit`.

## Special Test Cases

The repository has a separate example runner under `test/examples`. That path is not covered by the standard `mix test` suite.

Use the checked-in example command from `test/examples/README.md` when you need to validate the examples:

    MIX_ENV=test mix do ecto.drop, ecto.create, run test/examples/run.exs

That command rebuilds the test database before running the example script, so do not use it as a casual replacement for the ordinary `mix test` path.

## Contributor Maintenance Rules

This file stays useful only when it matches the current working tree.

- Update this guide in the same change whenever `mix.exs`, `.tool-versions`, `config/config.exs`, `test/test_helper.exs`, `priv/repo/migrations`, `coveralls.json`, or the repository CI workflows change the command surface or prerequisites.
- Keep this file self-contained. A beginner should be able to choose the right repository command from this file alone.
- Make other guides cite this file instead of restating repository commands. When another guide needs the exact command, root, repair step, or widening order, point it to the relevant section here.
- If you add a new repository-wide check, add it here first, then update the dependent guides and skills that route readers back here.
