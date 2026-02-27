---
trigger: always_on
description: Use when refactoring Elixir code.
---

## When to use

Use when the task is a refactor.

A refactor changes code structure while preserving intended behaviour.

If the task changes user-visible behaviour or adds a capability, treat that work as a feature task instead of a refactor task.

## Requirements

- When refactoring code, read the instructions in `.agent/datasets/refactor/code_smells` to identify code smells and anti-patterns.

- When refactoring code, read the instructions in `.agent/datasets/refactor/techniques` to identify refactoring techniques you can apply. The refactoring techniques explain the problem and the solution.

- Use any matching rules in `.windsurf/rules/elixir-refactor/` for the code being refactored.

## What to do

Before editing code, do the following in order.

1. Define one observable boundary that must stay the same.
2. Define how that boundary will be validated.
3. Identify one specific refactor problem or code smell in the current code.
4. Choose exactly one refactoring technique rule that matches the problem.
5. Apply only that one technique.
6. Re-run validation.
7. Update refactor documentation to reflect what changed and what was verified.

Do not skip the technique selection step.

Do not apply multiple techniques in one pass unless the first technique is completed, validated, and documented, and a new refactor pass is started.

### Boundary rule

State one concrete boundary before changing code.

A boundary must be observable and testable. Examples include a function result, API response shape, CLI output, persisted side effect, or message shape.

If you cannot state the preserved boundary clearly, stop and clarify before refactoring.

### Validation rule

State how you will verify the preserved boundary before changing code.

Use an existing automated test, a new automated test, or a repeatable manual check.

After the refactor, run the validation and confirm the boundary still holds.

### Technique selection rule

You must choose one named refactoring technique rule before changing code.

Choose the technique that best matches the identified problem or smell.

If no technique rule matches the problem, record the problem and stop until the approach is clear.

### Change scope rule

Keep the scope small enough to validate quickly.

Prefer one function, one clause, one module section, or one narrow call chain at a time.

Do not mix behaviour changes with structural cleanup in the same refactor pass.

### Documentation rule

Update the RefactorDoc (as described in the `.agent/REFACTORS.md`) document after each refactor pass.

Record:
- the boundary that was preserved
- the validation method used
- the technique selected
- the result of validation
- any constraints discovered

Do not leave the RefactorDoc document in a draft state if code was changed.

### Output expectations

A valid refactor pass ends with all of the following being true:

- A preserved boundary was stated before changes.
- A validation method was stated before changes.
- One refactoring technique was selected before changes.
- Validation was run after changes.
- Refactor documentation was updated.