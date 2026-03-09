## Goals

Each directive and each operator must have full defined behaviours through example mapping.
Together, the examples must show every valid data shape this API accepts. In other words,
every possible behaviour that this api has should be covered.

## Task and Key Files

Active task: Refactor `EctoShorts.Dynamics.Postgres.ScalarExpr` onto the compiler/builder path for the minimal scalar equality behaviour, then align focused Postgres and CommonFilters tests with that shape.

Key files:
- `PLAN.md`
- `DESIGN.md`
- `BINDING_REFACTOR_PLAN.md`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`
- `lib/ecto_shorts/dynamics/postgres.ex`
- `test/ecto_shorts/dynamics/postgres_test.exs`
- `test/ecto_shorts/common_filters_test.exs`

Document updates to track in the same change:
- Keep `DESIGN.md`, `PLAN.md`, and `BINDING_REFACTOR_PLAN.md` in sync as the task, proof path, or ownership changes.

## Milestones

Keep the Milestones section up to date as you work. Milestones track your larger portions of work, including required document creation and companion-document sync when the task changes shape.

### ScalarExpr Compiler Refactor

Status: Complete

- [x] `ScalarExpr` uses the compiler/builder path for the minimal scalar equality behaviour
- [x] `Postgres` keeps map and keyword-list handling at the container-routing layer
- [x] Focused `CommonFilters` and `Postgres` validation proves named and positional binding behaviour
- [x] The obsolete scalar specs test file is removed after the focused path is proven

Validation:

- [x] The focused refactor plan stays synchronized with `PLAN.md`
- [x] `mix test test/ecto_shorts/compiler_test.exs` passes
- [x] `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres_test.exs` passes

## Progress

Keep the progress section up to date as you work.

**Legend:**
[ ] not started
[~] in progress
[x] completed
 
- [x] Create or refresh the active planning document and keep its `Task and Key Files` section current.
- [x] Keep `PLAN.md` and every required companion planning document in sync with the current task.
- [x] Record the next concrete behaviour, example, or proof step here.
