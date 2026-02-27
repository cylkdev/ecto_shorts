---
name: run-test
description: Run tests for a specific app or all the child apps in an umbrella application then fix any failing tests
---

// turbo-all

# What to do 

Run tests for a specific app or all the child apps in an umbrella application then fix any failing tests

## How to do it

Use the following steps for this workflow. Do not skip any steps.

1. Ask the user which app to test. If they already specified one, use that. If they want all apps, run from the umbrella root.

2. Run the tests from the correct directory:

   - Single app: `mix test` from `apps/<app_name>/`
   - Umbrella-wide: `mix test` from the umbrella root
   - If the user provided a specific test file or line, use `mix test <path>` or `mix test <path>:<line>`

3. Wait for the command to finish and collect its output.

4. If all tests pass, report success and stop.

5. If tests failed, for each failure:
   - Read the failing test file and the corresponding source file
   - Determine whether the test or the source code is incorrect before choosing what to fix
   - Ask the user if unsure whether to fix the test or the implementation
   - Apply the minimal fix needed

6. After all fixes are applied, re-run the same test command to confirm everything passes.

7. If any tests still fail, repeat steps 5-6 until all tests pass.

8. Once all tests pass cleanly, report the final status to the user.