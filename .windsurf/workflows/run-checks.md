---
description: Chooses the right post-change verification path, then uses `.agent/PROJECT.md` and the specialized workflows to run the required repository checks.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing Elixir code, tests, or project configuration when you need to decide which verification checks to run.

Use it when you can classify the change as either rename-only or behaviour-, type-, or function-shape-affecting.

## When not to use

Do **not** use this workflow when the user explicitly asked for only Credo, only Dialyzer, or only tests. Use the matching workflow directly.

Do **not** use this workflow for documentation-only changes or other changes that did not affect Elixir code, tests, or project configuration.

## Definitions

- A `rename-only change` is a change that only renamed modules, functions, variables, files, aliases, or test names without changing implementation logic, control flow, runtime behaviour, function inputs, function outputs, types, specs, callbacks, or test meaning.
- A `function-shape change` is any change to a function's inputs, outputs, return shape, accepted arguments, or emitted error shape.
- A `behaviour change` is any change to runtime logic, query results, changeset validations, returned values, side effects, or user-observable results.
- A `type change` is any change to specs, types, callbacks, behaviours, structs, or other shapes that Dialyzer relies on.

## Source of truth

Use `.agent/PROJECT.md`, especially `Recommended Validation Paths`, as the source of truth for which repository checks belong to each change class.

Use the specialized workflows in this directory to execute the chosen path. Those workflows own their rerun discipline, but they should still take their exact commands and prerequisite repair steps from `.agent/PROJECT.md`.

## What to do

1. Classify the change before you run any checks.
   - Treat the change as a `rename-only change` when it only renamed modules, functions, variables, files, aliases, or test names and did not change implementation logic, control flow, behaviour, function inputs or outputs, types, specs, callbacks, or test meaning.
   - Treat the change as a `function-shape, type, or behaviour change` when it changed any function input or output, return shape, type, spec, callback, behaviour, struct shape, query behaviour, changeset behaviour, config-driven runtime behaviour, or test expectation because behaviour changed.
   - If you are not sure, choose the broader bucket and treat it as a `function-shape, type, or behaviour change`.

2. Read `.agent/PROJECT.md` and confirm the matching validation path in `Recommended Validation Paths`.

3. Run Credo only for a `rename-only change`.
   - Read `.windsurf/workflows/run-credo.md`.
   - Execute it exactly as written.
   - Stop after Credo only when the final run is clean.

4. Run all three specialized workflows for a `function-shape, type, or behaviour change`.
   - Read `.windsurf/workflows/run-credo.md` and execute it exactly as written.
   - Read `.windsurf/workflows/run-dialyzer.md` and execute it exactly as written.
   - Read `.windsurf/workflows/run-tests.md` and execute it exactly as written.
   - Run them in this order: Credo, then Dialyzer, then tests.

5. If the task also needs the repository's CI-equivalent coverage or statistics pass, use `CI-Equivalent Lint and Coverage Pass` in `.agent/PROJECT.md` after the required workflows above finish cleanly.

6. Report back which classification you chose and which workflows or additional PROJECT paths you ran.
   - If you chose the rename-only path, say that you ran only Credo because the change only renamed things.
   - If you chose the broader path, say that you ran Credo, Dialyzer, and tests because the change affected function shape, types, or behaviour.
