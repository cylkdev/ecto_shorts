---
description: Runs Credo in strict mode for the relevant Elixir Mix project, fixes lint and consistency issues, and repeats until the command passes cleanly.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing Elixir code, tests, or project configuration and you need to verify linting and code-quality checks with Credo.

## When not to use

Do **not** use this workflow to run Dialyzer or tests.

## Definitions

- A `Mix project` is any directory that contains `mix.exs`.
- An `umbrella project` is a Mix project whose child applications live under `apps/`.

## What to do

1. Choose the correct directory before you run anything.
   - If the change is limited to one child application inside an umbrella project, run Credo from that child application's directory.
   - If the change touches the umbrella root, shared configuration, or more than one child application, run Credo from the umbrella root.
   - If this is not an umbrella project, run Credo from the project root that contains `mix.exs`.

2. Make sure the project can load.
   - If dependencies are missing, run `mix deps.get`.
   - If the project does not compile, fix the compile error first. Credo output is not trustworthy until the project loads.

3. Run the linter:

       mix credo --strict

4. Fix the reported issues.
   - Start with the first issue in the output.
   - Make the smallest change that removes the issue.
   - Keep behaviour unchanged unless the task explicitly allows a behaviour change.
   - Re-run `mix credo --strict` after each coherent batch of fixes.

5. Widen the scope only if the work widened.
   - If you started in one child application and later changed shared or umbrella-level files, re-run `mix credo --strict` from the umbrella root.

6. Stop only when the chosen scope passes with exit code `0` and no Credo issues.

## Report back

State the directory you used, the exact command you ran, and whether the final run was clean.
