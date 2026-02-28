---
name: code-style-review
description: Performs a systematic code style review of the codebase against the project's style rules in `.agent/code-styles/`. Use this skill when reviewing code style or when you want to ensure the codebase adheres to the style guidelines.
---

## When to use

Use this skill when you need to review or enforce the project's code style rules. Use it after implementing a feature, during a pull request review, or when you want a full pass to ensure the codebase conforms to the style guidelines.

## When not to use

Do **not** use this skill for implementing features. Use the `write-exec-plan` skill for that. Do **not** use this skill for behaviour-preserving refactors. Use the `refactor` skill for that. Do **not** use this skill for architecture reviews. Use the `architecture-review` skill for that.

## What to do

Read all files in `.agent/code-styles/` and load them into your context. If they already exist in context, read them again to refresh your memory. Treat them as the source of truth.

### 1. Load the style rules

Read every file in `.agent/code-styles/`. The current files are:

- **ABSINTHE.md** - Absinthe GraphQL conventions.
- **CODE_RELATED_ANTI_PATTERNS.md** - code-level anti-patterns to avoid.
- **DESIGN_RELATED_ANTI_PATTERNS.md** - design-level anti-patterns to avoid.
- **META_PROGRAMMING_ANTI_PATTERNS.md** - meta-programming anti-patterns to avoid.
- **NAMING_CONVENTIONS.md** - naming rules for modules, functions, variables, and files.
- **PROCESS_RELATED_ANTI_PATTERNS.md** - process and OTP anti-patterns to avoid.
- **PUBLIC_API_AND_INTERFACES.md** - rules for public API surface and interfaces.
- **STRUCT_ANTI_PATTERNS.md** - struct-related anti-patterns to avoid.
- **TESTING.md** - test style and structure rules.

### 2. Determine scope

Decide whether this is a focused review (only changed files) or a full pass (the whole codebase). State which scope you picked so the reviewer can see it.

### 3. Review the code

For each file in scope, compare it against the loaded style rules. Check for violations of naming conventions, anti-patterns, public API rules, testing rules, and any other rules defined in the style docs.

### 4. Fix violations

Apply fixes as you find them. Do not "fix style later". Each change must leave the code in a valid, compilable state.

### 5. Run formatting and linting

Run the repo's formatting and linting commands:

- `mix format`
- `mix credo --strict`

Fix any issues reported. Repeat until both pass with no warnings or errors.

### 6. Summarize

List the rules you applied and the files you changed. If you found no violations, state that explicitly.
