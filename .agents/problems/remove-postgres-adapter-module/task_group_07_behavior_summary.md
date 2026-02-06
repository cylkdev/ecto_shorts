# Task Group 07 Behavior Summary

## Expected behavior (no query semantics change)
- `EctoShorts.QueryBuilder.Dynamics` still produces the same `dynamic/2` predicates for common/scalar/array operators.
- Dispatch still happens via `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.{Common,Scalar,Array}.dynamic_field_expr/3`.

## Structural changes
- No compile callbacks:
  - `@after_compile` / `__after_compile__/2` removed.
  - `Module.create/3` removed.
- `Compiled` modules are explicit:
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Common`
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Scalar`
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Array`
- Clause generation is performed by `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: ...` inside each compiled module.

