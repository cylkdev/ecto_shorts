---
name: run-checks
description: Runs the project's standard quality checks after a change by running Credo, Dialyzer, and the test suite, then fixing any issues until everything passes.
---

## When to use

Use this skill after implementing a feature or making a change to the codebase. It validates that the code matches the project's coding standards, type safety, and test coverage.

## When not to use

Do **not** use this skill as a substitute for a code style review. Use the `code-style-review` skill for that. Do **not** use this skill to run a single tool in isolation — invoke the tool directly instead.

## What to do

1. Run the following commands in parallel:

- `mix credo --strict`
- `mix dialyzer`
- `mix test`

2. Fix any warnings or errors reported by `mix credo --strict`.

3. Fix any warnings reported by `mix dialyzer`.

4. Fix any failures reported by `mix test`.

5. Run all three commands again from step 1. Repeat until all pass with no warnings or errors.
