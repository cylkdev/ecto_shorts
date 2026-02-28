---
description: Runs the project's standard quality checks (Credo, Dialyzer, tests) after a change, then fixes any issues until everything passes.
auto_execution_mode: 3
---

// turbo-all

# What to do

Run the project's standard quality checks after implementing a feature or making a change to the codebase. Validate that the code matches the project's coding standards, type safety, and test coverage.

## When not to use

Do **not** use this workflow as a substitute for a code style review. Use the `code-style-review` workflow for that. Do **not** use this workflow to run a single tool in isolation — invoke the tool directly instead.

## How to do it

Use the following steps for this workflow. Do not skip any steps.

1. Run the following commands in parallel:

   - `mix credo --strict`
   - `mix dialyzer`
   - `mix test`

2. Wait for all commands to finish and collect their output.

3. If all three pass with no warnings or errors, report success and stop.

4. If `mix credo --strict` reported warnings or errors:
   - Read the flagged source files
   - Apply the minimal fix needed for each issue
   - Do not change unrelated code

5. If `mix dialyzer` reported warnings:
   - Read the flagged source files
   - Apply the minimal fix needed for each warning
   - Do not change unrelated code

6. If `mix test` reported failures:
   - Read the failing test file and the corresponding source file
   - Determine whether the test or the source code is incorrect before choosing what to fix
   - Ask the user if unsure whether to fix the test or the implementation
   - Apply the minimal fix needed

7. After all fixes are applied, re-run all three commands from step 1.

8. Repeat steps 4-7 until all three commands pass with no warnings or errors.

9. Once everything passes cleanly, report the final status to the user.
