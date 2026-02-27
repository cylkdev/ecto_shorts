---
name: run-checks
description: Run after implementing a feature or making a system change.
---

// turbo-all

## Requirements

- Read `.agent/TESTS.md` and follow it to the _letter_ when writing tests.

- Read the files in `.agent/datasets/code-styles` and use those guidelines when writing code.

- Execute tasks in a single uninterrupted run. Do not pause for approval. Do not create partial diffs. Apply all required edits across all files. Present a single final change set when the task is fully complete.

# What to do

1. Run the following commands in parallel:

- `mix credo --strict`
- `mix dialyzer`
- `mix test`

2. Wait for all three commands to finish.

3. Fix any issues reported by credo.

4. Fix any issues reported by dialyzer.

5. Fix any issues reported by tests.

6. Run the commands again to ensure all issues are resolved.

7. Start over from step 1 until all commands pass.