---
trigger: always_on
description: Use for each task involving feature planning, implementation, refactoring, or quality checks.
---

## When to use

Use for each task involving feature planning, implementation, refactoring, or quality checks.

## What to do

Use an `ExecPlan` when planning features, writing features, or refactoring code.

Use a `BehaviourSpec` when planning, implementing, or refactoring to define the intended behaviour of the feature.

Use an `ExampleDoc` when behaviour is unclear, ambiguous, or has many variations and you need concrete examples before writing or updating a `BehaviourSpec`.

Use a `RefactorDoc` when refactoring code.

Use an `ADR` when an important technical or architecture decision is made.

### Maintenance

Create or update the required documents as soon as the task starts.

Write them to disk when they are used. Do not keep them only in chat.

Keep them up to date as you learn, make decisions, implement changes, and verify results.

Reuse and update existing matching documents instead of creating duplicates.

These documents are living records. They should reflect the current state of the work at all times.

### Outputs

Write all files to `docs/` at the root of the project.

- `ADR`: `docs/adrs/YYYY-MM-DD-adr-<feature-slug>.md`
- `BehaviourSpec`: `docs/behaviors/YYYY-MM-DD-behaviour-<feature-slug>.md`
- `ExampleDoc`: `docs/examples/YYYY-MM-DD-example-<feature-slug>.md`
- `ExecPlan`: `docs/plans/YYYY-MM-DD-plan-<feature-slug>.md`
- `RefactorDoc`: `docs/refactors/YYYY-MM-DD-refactor-<feature-slug>.md`
