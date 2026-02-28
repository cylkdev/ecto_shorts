---
auto_execution_mode: 3
description: Run this workflow after you implement a feature or make a system change. This workflow updates test coverage by using ExCoveralls to show which source lines are not executed by tests, and then adding tests until all lines are covered.
---

// turbo-all

## Goal

You will generate a text coverage report, find uncovered lines, write tests that execute those lines, and repeat until the report shows full coverage.

## Requirements

- Read `.agent/TESTS.md` and follow it to the _letter_ when writing tests.

- Read the files in `.agent/code-styles` and use those guidelines when writing code.

## Prerequisites

- You must be in the root directory of an Elixir Mix project.

- You must have ExCoveralls configured as a dependency in mix.exs, and you must be able to run tests with mix test.

- You must know whether your project is an umbrella project:

  - A non-umbrella project has a `mix.exs` at the repository root and does not have an `apps/` directory that contains child apps.

  - An umbrella project has a `mix.exs` at the repository root and an `apps/` directory that contains one or more child apps, each with its own `mix.exs`.

### Commands to use

- Use `mix coveralls.detail` to print a detailed coverage report that includes per-file line coverage information.

- Use `mix coveralls.detail --umbrella` to print a single coverage report for the whole umbrella.

- Use `> tmp/coveralls.txt` to write the report output to a file named `coveralls.txt`, for example `mix coveralls.detail > tmp/coveralls.txt`.

- Use `mix test path/to/test_file.exs` to run one test file.

## What to do

1. Confirm the project type.
   - If an `apps/` directory exists at the project root and it contains child applications, treat the project as an umbrella project.
   - Otherwise, treat the project as a non-umbrella project.

2. Generate a coverage report and save it to `tmp/coveralls.txt`.
   - For a non-umbrella project, run `mix coveralls.detail > tmp/coveralls.txt`.
   - For an umbrella project, run `mix coveralls.detail --umbrella > tmp/coveralls.txt`.

3. Read `tmp/coveralls.txt`.

4. Find the single source file with the most uncovered lines.
   - Use the file path and uncovered line information shown in the report.
   - If the report shows multiple files with the same number of uncovered lines, pick the first one listed.

5. Open that source file.

6. Choose one uncovered block to cover next.
   - A block is “one uncovered block” when it is one contiguous group of uncovered lines.
   - Prefer a block inside one function clause or one small helper function.

7. Identify one callable entry point that should execute the uncovered block.
   - A callable entry point is a public function in the same module or a public function in another module that calls into it.
   - A public function in Elixir is a function defined with `def`, not `defp`.

8. Decide where the new test will live.
   - If a matching test file already exists under `test/` for that module, add the test there.
   - If no matching test file exists, create one under `test/` that mirrors the source path.
   - Example: if the source file is `lib/my_app/widget.ex`, create `test/my_app/widget_test.exs`.

9. Create or update the test module.
   - Start the file with `ExUnit.start()` only if your project does not already start ExUnit globally in `test/test_helper.exs`.
   - Define the test module name to match the source module name with `Test` appended.
   - Example: if the source module is `MyApp.Widget`, name the test module `MyApp.WidgetTest`.

10. Write one test that executes the uncovered block.
   - Name the test after the behavior it verifies.
   - Call the entry point you identified in step 7 with inputs that will cause the uncovered lines to run.
   - Add at least one assertion that checks a user-observable result.
   - A user-observable result is a return value, a raised error, a sent message you can assert, or a persisted change you can query.
   - Do not write a test that only asserts `true` or only checks that the code “does not crash” unless the intended behavior is “does not raise”.

11. Run only the test file you changed until it passes.
   - Run `mix test path/to/test_file.exs`.
   - If the test fails, change the test inputs or fix the code, and run the same command again.
   - Stop when the test file passes.

12. Regenerate `tmp/coveralls.txt` using the same coverage command you used in step 2.

13. Confirm progress for the chosen uncovered block.
   - Re-open `tmp/coveralls.txt`.
   - Confirm that the file you targeted has fewer uncovered lines than before, or confirm that the specific uncovered lines you targeted are no longer listed as uncovered.

14. Repeat the loop.
   - Go back to step 4 and repeat until the report shows no uncovered lines.
