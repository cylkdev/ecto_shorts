## Goals

Each directive and each operator must have full defined behaviours through example mapping.
Together, the examples must show every valid data shape this API accepts. In other words,
every possible behaviour that this api has should be covered.

## Task and Key Files

Active task: Simplify the CommonExpr binding implementation so one generated module handles both named bindings and positional bindings, then align the narrow Postgres routing, generated artifact, and focused tests with that shape.

Key files:
- `PLAN.md`
- `BINDING_REFACTOR_PLAN.md`
- `lib/ecto_shorts/dynamics/postgres/common_expr.ex`
- `lib/ecto_shorts/compiler.ex`
- `lib/ecto_shorts/generator.ex`
- `lib/ecto_shorts/dynamics/postgres.ex`
- `test/ecto_shorts/common_filters_test.exs`

Document updates to track in the same change:
- Create or refresh the active root planning artifact for this refactor and keep its `Task and Key Files` section current.
- Keep `PLAN.md` and `BINDING_REFACTOR_PLAN.md` in sync as the task, proof path, or ownership changes.

## Milestones

Keep the Milestones section up to date as you work. Milestones track your larger portions of work, including required document creation and companion-document sync when the task changes shape.

### Binding Simplification

Status: Complete

- [x] One generated CommonExpr module handles both `{:as, ...}` and `{:at, ...}`
- [x] `:modes` is removed from the active compiler and generator path
- [x] The tracked generated CommonExpr artifact matches the one-module design
- [x] Focused CommonExpr and CommonFilters validation proves named and positional binding behaviour

Validation:

- [x] The focused refactor plan stays synchronized with `PLAN.md`
- [x] `mix compile` succeeds for the touched path
- [x] Focused tests for CommonExpr/CommonFilters pass

## Progress

Keep the progress section up to date as you work.

**Legend:**
[ ] not started
[~] in progress
[x] completed
 
- [x] Create or refresh the active planning document and keep its `Task and Key Files` section current.
- [x] Keep `PLAN.md` and every required companion planning document in sync with the current task.
- [x] Record the next concrete behaviour, example, or proof step here.
