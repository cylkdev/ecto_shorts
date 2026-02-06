# Task Group 06 Diff Summary

## Summary
Simplified the `QueryBuilder.Dynamics.Compiler` API by removing the threaded “kind” tag entirely and removing emitter indirection. Clause compilation now lives directly in `EctoShorts.QueryBuilder.Dynamics.Compiler` and generates fixed `dynamic_field_expr/3` clauses.

## Key changes
- Removed `:kind` from clause spec data:
  - `EctoShorts.QueryBuilder.Dynamics.Compiler.ClauseSpec` no longer has a `:kind` field and no longer validates a `:kind` option.
- Removed emitter indirection:
  - Deleted `lib/ecto_shorts/query_builder/dynamics/compiler/emitters/dynamic_field_expr.ex`.
  - `EctoShorts.QueryBuilder.Dynamics.Compiler.clause_ast/1` now emits quoted `def dynamic_field_expr/3` clauses directly.
- Merged clause compilation into the compiler entrypoint:
  - Deleted `lib/ecto_shorts/query_builder/dynamics/compiler/clause_builder.ex`.
  - `EctoShorts.QueryBuilder.Dynamics.Compiler.__after_compile__/2` now compiles `Adapter.Compiled.{Common,Scalar,Array}` modules explicitly (no loop over `:common/:scalar/:array`).
- Updated `EctoShorts.QueryBuilder.Dynamics.Postgres`:
  - Kept a single `clause_specs/4` callback implementation.
  - Uses the `context` (compiled module) to choose which spec set to return (Common vs Scalar vs Array) without threading a separate tag argument.
- Updated affected tests to remove old “kind” assertions and emitter/ClauseBuilder references.

