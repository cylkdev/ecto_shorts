# RefactorPlan: ScalarExpr Compiler Refactor

## Summary

Refactor `EctoShorts.Dynamics.Postgres.ScalarExpr` onto the compiler/builder path for the minimal scalar equality behaviour. Keep `ScalarExpr` as a thin compiler-backed wrapper, move the generated clause definitions into `ScalarExprBuilder`, keep the narrow `Postgres` routing aligned with tuple-only expression-module inputs, and prove the result through the focused `CommonFilters` and `Postgres` tests.

## Task and Key Files

Active task: Refactor `EctoShorts.Dynamics.Postgres.ScalarExpr` onto the compiler/builder path for the minimal scalar equality behaviour, then align the narrow Postgres routing and focused tests around that shape.

Key files:
- `BINDING_REFACTOR_PLAN.md`
- `PLAN.md`
- `DESIGN.md`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`
- `lib/ecto_shorts/dynamics/postgres.ex`
- `test/ecto_shorts/dynamics/postgres_test.exs`
- `test/ecto_shorts/common_filters_test.exs`

Document updates to track in the same change:
- Keep `DESIGN.md`, `PLAN.md`, and `BINDING_REFACTOR_PLAN.md` synchronized while this refactor is active.

## Trigger for Using This Document

Observed facts that triggered this document choice:
- `ScalarExpr` was still hand-written while `CommonExpr` already used the compiler/builder path.
- The failing scalar equality path showed that the current `ScalarExpr` shape was not aligned with the intended compiler-backed expression-module design.
- The user explicitly narrowed scope to the minimal scalar equality behaviour and asked for `ScalarExpr` to follow the `CommonExpr` compiler/builder pattern.
- The refactor target is mostly structural, but it also closes a narrow failing behaviour at the same time.
- The repository already contains `PLAN.md`, which instructs contributors to create or refresh a companion root planning document when the work hands off to one.

Reasoning path from those facts to this document:
- Because the main work is moving `ScalarExpr` onto an existing compiler/builder architecture, the dominant concern is still structural shape rather than inventing a broad new behaviour surface.
- The work still needs an execution sequence, validation path, and synchronized companion-document maintenance, so a root planning artifact is necessary rather than keeping the plan only in chat.
- Keeping that sequence in a dedicated refactor plan is safer than encoding it only in `PLAN.md` because the task has concrete file boundaries, focused validation checkpoints, and cleanup steps that a novice implementer needs in one place.

Nearby document types considered and rejected:
- `ExecPlan`: rejected because the main purpose is not to introduce a new behaviour boundary; it is to restructure an existing binding implementation without intentionally changing observable behaviour.
- `BehaviourSpecDoc`: rejected because the missing piece is not uncertainty about user-observable rules. The desired behaviour is already known; the problem is structural complexity.
- `InvestigationLog`: rejected because the investigation is already complete enough to choose an implementation path. The task is not diagnosis anymore.

Replication rule:
- Use a `RefactorPlan` when repository evidence shows the requested work is primarily a structural move onto an existing architecture, even if a narrow failing behaviour is fixed along the way, and the work needs an explicit implementation sequence with validation checkpoints.

## Implementation Outline

1. Save and maintain this root refactor plan and `PLAN.md`.
2. Refactor `ScalarExpr` into a thin compiler-backed wrapper and add `ScalarExprBuilder`.
3. Keep `Postgres` responsible for container recursion so expression modules only receive tuple inputs.
4. Add focused validation for `CommonFilters` and direct `Postgres` named-binding and positional-binding scalar equality behaviour.
5. Remove the obsolete scalar specs test file and record the focused validation results.

## Progress

Legend:
- `[ ]` not started
- `[~]` in progress
- `[x]` completed

- [x] Create or refresh the active planning document and keep its `Task and Key Files` section current.
- [x] Keep `BINDING_REFACTOR_PLAN.md` and `PLAN.md` synchronized with the active refactor.
- [x] Refactor `ScalarExpr` into a compiler-backed wrapper with `ScalarExprBuilder`.
- [x] Keep `Postgres` responsible for recursive container handling before tuple-only expression-module dispatch.
- [x] Add or align focused tests for direct `Postgres` runtime behaviour and the narrow `CommonFilters` path.
- [x] Remove the obsolete scalar specs test file.
- [x] Run focused validation and record any unrelated remaining failures.

## Validation Notes

- `mix test test/ecto_shorts/compiler_test.exs` passed with 7 tests and 0 failures.
- `mix test test/ecto_shorts/common_filters_test.exs` passed with 8 tests and 0 failures.
- `mix test test/ecto_shorts/dynamics/postgres_test.exs` passed with 2 tests and 0 failures.
- `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres_test.exs` passed with 10 tests and 0 failures.
- Unrelated warnings still come from `lib/ecto_shorts/common_filters.old.ex`.
- Recompilation emits a module redefinition warning for `EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled` during focused test runs.
