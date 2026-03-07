---
description: Runs Elixir tests from the most specific relevant scope to the widest, fixes failures, and repeats until the required test scopes pass.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing behaviour, queries, changesets, public APIs, or tests and you need to prove the change with Elixir test runs.

## When not to use

Do **not** use this workflow to run Credo or Dialyzer.

## Definitions

- A `Mix project` is any directory that contains `mix.exs`.
- An `umbrella project` is a Mix project with child applications under `apps/`.
- A `child application` is one application inside an umbrella project.
- `ExUnit` is Elixir's built-in test framework. You run it with `mix test`.

## What to do

1. Choose the directory that owns the test.
   - If you know the failing test file, move upward from that file until you reach the nearest directory that contains `mix.exs`. Run test commands from there.
   - Use the umbrella root only when the target test lives under the umbrella root `test/` directory or the change genuinely spans multiple child applications.
   - If this is not an umbrella project, run from the project root that contains `mix.exs`.

2. Start from the most specific useful command.
   - If you know the exact failing line, run:

         mix test path/to/file_test.exs:LINE

   - If you know the failing file but not the line, run:

         mix test path/to/file_test.exs

   - If you only know the owning application, run:

         mix test

   - Widen to the umbrella root `mix test` only when the change affects root-level tests or multiple child applications.

3. If `mix test` cannot reach the real failing test, repair setup first.
   - Dependency failure: run `mix deps.get` and then `mix deps.compile`.
   - Compile failure: fix the first project file named in the error output or stacktrace, then rerun the same test command.
   - Configuration failure: inspect `config/config.exs`, `config/test.exs`, `config/runtime.exs`, `test/test_helper.exs`, and `mix.exs` for the missing setting or environment variable.
   - Database failure: check `mix.exs` aliases first. If the project uses Ecto tasks and no alias already covers setup, run `mix ecto.create` and `mix ecto.migrate`.
   - Required service failure: search the repository for the service name and start the dependency the way the repository already documents or scripts it.
   - After every setup repair, rerun the same smallest test command. Do not widen scope until ExUnit reaches the real test failure.

4. Once ExUnit reaches the real failing test, fix the failure.
   - Read the failing test name, assertion line, diff, or first useful project stacktrace line.
   - Decide whether the implementation is wrong or the test expectation is wrong before you edit anything.
   - If you conclude that the test expectation is wrong, or the intended behaviour is still unclear, stop and ask the user before changing the test.
   - Make the smallest change that resolves the failure.

5. Re-run the same smallest command until it passes with `0 failures`.

6. Widen from the most specific scope to the widest relevant scope.
   - Re-run the whole test file after a line-targeted command passes.
   - Re-run `mix test` from the owning Mix project after the file passes.
   - Re-run `mix test` from the umbrella root only when the change affects multiple child applications, root-level tests, or shared code used across the umbrella.

7. Repeat the fix-and-rerun cycle until every required scope passes.

8. Stop only when the widest relevant test command passes with `0 failures`.

## Report back

State the directories and commands you ran, which scope was the smallest, which scope was the widest, and whether the final run was clean.
