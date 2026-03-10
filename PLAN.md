# Simplify `ScalarExprBuilder` by Moving Shape Branching into Blueprint Bodies

## Summary
Refactor `ScalarExprBuilder` so each public scalar operator key generates only the minimum blueprint set and handles term-shape branching inside the quoted blueprint body. Keep the current public operator keys, keep `Postgres` normalization as already approved, and make negation generic with `Helpers.negated_expr/1` semantics.

## Implementation Changes
- Rework `ScalarExprBuilder.specs_for/4` to emit only two blueprints per operator key:
  - normal: `head: {key, value_var}`
  - negated: `head: {:not, {key, value_var}}`
- Remove the current extra blueprint heads for:
  - `nil`
  - `{:lower, value}`
  - `{:upper, value}`
  - their negated variants
- In each blueprint body, use a quoted `case` on `value_var` and unquote the `Helpers.dyn_expr(...)` result for the matching branch.
- Keep `keys/0` as the current public operator list:
  - `:==`, `:eq`, `:!=`, `:ne`, `:>`, `:>=`, `:<`, `:<=`, `:gt`, `:gte`, `:lt`, `:lte`, `:in`, `:like`, `:ilike`
- Partition the branching logic by operator family inside `specs_for/4`:
  - equality / inequality:
    - `nil`
    - `{:lower, value}`
    - `{:upper, value}`
    - list semantics for scalar fields
    - plain scalar value
  - comparisons:
    - plain scalar value
    - raise on `nil`
  - `:in`:
    - list value only
  - `:like` / `:ilike`:
    - scalar string
    - list value as `ANY`
- Keep `expr_for/3` as the positive-expression builder, but call it from inside the branch bodies with the concrete branch shape.
- Keep negation generic across all scalar families:
  - negated blueprint body wraps the positive expression with `Helpers.negated_expr/1`
  - do not add operator-flip special cases in the builder
- Preserve the current approved structure of:
  - [postgres.ex](/Users/kurthogarth/Documents/GitHub/ecto_shorts/lib/ecto_shorts/dynamics/postgres.ex)
  - [scalar_expr.ex](/Users/kurthogarth/Documents/GitHub/ecto_shorts/lib/ecto_shorts/dynamics/postgres/scalar_expr.ex)

## Test Plan
- Update direct scalar tests in [scalar_expr_test.exs](/Users/kurthogarth/Documents/GitHub/ecto_shorts/test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs) to match generic negation where needed:
  - `not (field == value)` instead of `field != value`
  - `not (field != value)` instead of `field == value`
  - `not (field in list)` instead of `field not in list`
- Keep the nested `Postgres.build_dynamic/4` normalization test in [postgres_test.exs](/Users/kurthogarth/Documents/GitHub/ecto_shorts/test/ecto_shorts/dynamics/postgres_test.exs) and expect generic negation there as well.
- Focused validation after each small implementation step:
  - `mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
  - `mix test test/ecto_shorts/dynamics/postgres_test.exs`
  - then `mix test test/ecto_shorts/common_filters/scalar_filter_test.exs`

## Assumptions
- Generic negation applies to all scalar operator families, not just equality / inequality.
- `ScalarExprBuilder` should stay a single builder with the current public operator keys rather than introducing internal family keys or multiple builders.
- The current blueprint explosion is a design problem in `specs_for/4`, not a reason to change the compiler, `Postgres`, or the public scalar term shape.
