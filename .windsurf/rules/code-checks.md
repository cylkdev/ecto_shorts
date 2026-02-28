---
trigger: always_on
---

## What to do

After implementing a feature or making a change to the codebase, run the code check workflow.

## Code Check Workflow

Use these steps to perform a code check:

1. Run the following commands in parallel:

  - `mix credo --strict`
  - `mix dialyzer`
  - `mix test`

2. Wait for all commands to complete.

3. Fix any issues reported by credo.

4. Fix any issues reported by dialyzer.

5. Fix any issues reported by tests.

6. Repeat from step 1 until all commands pass.