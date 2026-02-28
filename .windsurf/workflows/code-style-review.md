---
description: Performs a code style review of changed or specified files against the project's style rules in `.agent/code-styles/`. Use this workflow after implementing a feature or when you want to ensure code adheres to the style guidelines.
auto_execution_mode: 3
---

// turbo-all

## What to do

1. Read every file in `.agent/code-styles/`. Treat them as the source of truth.

2. Determine scope: review only the files that changed (default) or the whole codebase if the user requests a full pass. State which scope you picked.

3. For each file in scope, compare it against the loaded style rules. Check for violations of naming conventions, anti-patterns, public API rules, struct rules, testing rules, and any other rules defined in the style docs.

4. Fix violations as you find them. Each change must leave the code in a valid, compilable state.

5. Run `mix format` and fix any formatting issues.

6. Run `mix credo --strict` and fix any issues reported. Repeat steps 5–6 until both pass.

7. Summarize the rules you applied and the files you changed. If you found no violations, state that explicitly.
