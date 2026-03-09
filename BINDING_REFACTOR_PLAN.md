# RefactorPlan: One-Module Binding Simplification

## Summary

Refactor the active `EctoShorts.Dynamics.Postgres.CommonExpr` compiler path so one generated module handles both named bindings and positional bindings. Remove `:modes` from the active compiler and generator flow, expose a simpler `CommonExpr.dynamic_expr/3` public API, keep a narrow compatibility wrapper for callers still passing `opts`, and keep the tracked generated artifact synchronized with the new one-module shape.

## Task and Key Files

Active task: Simplify the CommonExpr binding implementation so one generated module handles both named bindings and positional bindings, then align the narrow Postgres routing and focused tests around that single-binding-selector API.

Key files:
- `BINDING_REFACTOR_PLAN.md`
- `PLAN.md`
- `lib/ecto_shorts/dynamics/postgres/common_expr.ex`
- `lib/ecto_shorts/compiler.ex`
- `lib/ecto_shorts/generator.ex`
- `lib/ecto_shorts/dynamics/postgres.ex`
- `lib/ecto_shorts/dynamics/postgres/common_expr/spec.ex`
- `priv/generated/dynamics/postgres/common_expr/spec/compiled.ex`
- `test/ecto_shorts/common_filters_test.exs`

Document updates to track in the same change:
- Keep `BINDING_REFACTOR_PLAN.md` and `PLAN.md` synchronized while this refactor is active.
- Update the tracked generated artifact notes in this plan if the generated CommonExpr file path changes.

## Trigger for Using This Document

Observed facts that triggered this document choice:
- The active `CommonExpr` design split named bindings and positional bindings into separate generated modules through a `:modes` option.
- The user explicitly asked for a simplification that makes binding behaviour easier for humans to reason about and reduces moving parts.
- The refactor target is structural rather than a new user-visible feature. The intended behaviour stays the same while the internal module and generation shape becomes simpler.
- The repository already contains `PLAN.md`, which instructs contributors to create or refresh a companion root planning document when the work hands off to one.

Reasoning path from those facts to this document:
- Because the main work is a structural simplification of existing behaviour, the correct primary planning artifact is a `RefactorPlan`.
- The work still needs an execution sequence, validation path, and synchronized companion-document maintenance, so a root planning artifact is necessary rather than keeping the plan only in chat.
- A dedicated refactor plan is safer than encoding this work only in `PLAN.md` because the task has concrete code boundaries, generated artifacts, and validation checkpoints that a novice implementer needs in one place.

Nearby document types considered and rejected:
- `ExecPlan`: rejected because the main purpose is not to introduce a new behaviour boundary; it is to restructure an existing binding implementation without intentionally changing observable behaviour.
- `BehaviourSpecDoc`: rejected because the missing piece is not uncertainty about user-observable rules. The desired behaviour is already known; the problem is structural complexity.
- `InvestigationLog`: rejected because the investigation is already complete enough to choose an implementation path. The task is not diagnosis anymore.

Replication rule:
- Use a `RefactorPlan` when repository evidence shows the requested work is primarily structural, should preserve observable behaviour, and needs an explicit implementation sequence with validation checkpoints.

## Implementation Outline

1. Save and maintain this root refactor plan and `PLAN.md`.
2. Collapse `CommonExpr` to one generated module and expose the simpler public wrapper API.
3. Remove `:modes` from the active compiler and generator flow.
4. Add focused validation for named-binding and positional-binding behaviour.
5. Regenerate the tracked CommonExpr generated file and remove the obsolete named/positional generated artifacts.

## Progress

Legend:
- `[ ]` not started
- `[~]` in progress
- `[x]` completed

- [x] Create or refresh the active planning document and keep its `Task and Key Files` section current.
- [x] Keep `BINDING_REFACTOR_PLAN.md` and `PLAN.md` synchronized with the active refactor.
- [x] Refactor `CommonExpr` to one generated module with a simple binding-selector API.
- [x] Remove `:modes` from the active compiler and generator flow.
- [x] Add or align focused tests for CommonExpr runtime behaviour and the narrow CommonFilters path.
- [x] Regenerate the tracked CommonExpr generated artifact and remove the obsolete generated files.
- [x] Run focused validation and record any unrelated remaining failures.

## Validation Notes

- `mix compile` succeeded after the one-module CommonExpr refactor.
- `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres/common_expr_runtime_test.exs test/ecto_shorts/compiler/unified_binding_generation_test.exs` passed with 7 tests and 0 failures.
- `rg -n "\\bmodes\\b" lib/ecto_shorts` returned no active matches.
- `priv/generated/dynamics/postgres/common_expr/spec` now contains only `compiled.ex`.
- `mix test` still fails outside this refactor because the broader dynamic-expression surface is incomplete in the current working tree.
  Remaining unrelated blockers observed in that full-suite run:
  `EctoShorts.Dynamics.convert_to_dynamic/3` and `/4` are not available to the legacy query-builder tests.
  Array/scalar filter tests still receive `nil` dynamics because `EctoShorts.Dynamics.Postgres.ArrayExpr` and `EctoShorts.Dynamics.Postgres.ScalarExpr` are still stubs in this workspace.
  `test/ecto_shorts/compiler/common_expr_specs_test.exs` still contains an invalid nested alias form (`as: CommonExprBuilders`).
