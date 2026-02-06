# Task Group 08 Diff Summary

## Summary
Implemented the predictable `X.Compiled` module pattern for Dynamics expression builders and simplified the Postgres adapter boundary so `EctoShorts.QueryBuilder.Dynamics` no longer owns the “common operators” list or Scalar/Array routing decisions.

## Key changes
- `lib/ecto_shorts/query_builder/dynamics/compiler.ex`
  - `__using__/1` now defines `X.Compiled` (generated `dynamic_field_expr/3` clauses) and also defines `X.dynamic_field_expr/3` delegating to `X.Compiled`.
- `lib/ecto_shorts/query_builder/dynamics/postgres.ex`
  - Removed `Postgres.Compiled.*` ownership.
  - Added `operators/0` and `build_dynamic_expression/4` (and no other public API).
- `lib/ecto_shorts/query_builder/dynamics.ex`
  - Removed hardcoded `@common_operators` and Scalar-vs-Array routing.
  - Uses `Postgres.operators/0` to detect adapter operator keys and delegates predicate building to `Postgres.build_dynamic_expression/4`.
- Postgres expression modules now own their own compiled output:
  - `lib/ecto_shorts/query_builder/dynamics/postgres/common_expr.ex`
  - `lib/ecto_shorts/query_builder/dynamics/postgres/scalar_expr.ex`
  - `lib/ecto_shorts/query_builder/dynamics/postgres/array_expr.ex`
  - Each module is now a `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: ...` one-liner (no manual delegator function, no `@compile` suppression).
- Specs re-homed to match the new `X.Specs` convention (one module per file):
  - Added `lib/ecto_shorts/query_builder/dynamics/postgres/common_expr/specs.ex`
  - Moved/renamed:
    - `lib/ecto_shorts/query_builder/dynamics/postgres/specs/scalar_exprs.ex` → `lib/ecto_shorts/query_builder/dynamics/postgres/scalar_expr/specs.ex`
    - `lib/ecto_shorts/query_builder/dynamics/postgres/specs/array_exprs.ex` → `lib/ecto_shorts/query_builder/dynamics/postgres/array_expr/specs.ex`
  - Deleted `lib/ecto_shorts/query_builder/dynamics/postgres/specs/common_exprs.ex`
- Tests updated for renamed specs modules:
  - `test/ecto_shorts/query_builders/postgres/dynamics/specs/expr_builder_group_specs_test.exs`

