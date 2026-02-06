# Task Group 05 Diff Summary

## Summary
Refactored `lib/ecto_shorts/query_builder/dynamics/**` to remove the older `Expression`/`ClauseAdapter`/`ClauseEmitter` shape, replacing it with a single compiler entrypoint (`EctoShorts.QueryBuilder.Dynamics.Compiler`) and a non-plural Postgres namespace (`EctoShorts.QueryBuilder.Dynamics.Postgres`).

## Key changes
- Moved `EctoShorts.QueryBuilder.ParamPreprocessor` to `EctoShorts.QueryBuilder.Dynamics.ParamPreprocessor` (`lib/ecto_shorts/query_builder/dynamics/param_preprocessor.ex`) and updated call sites/tests.
- Replaced `EctoShorts.QueryBuilder.Dynamics.Expression.*` with `EctoShorts.QueryBuilder.Dynamics.Compiler.*`:
  - New compiler entrypoint: `lib/ecto_shorts/query_builder/dynamics/compiler.ex`
  - Moved helpers to `lib/ecto_shorts/query_builder/dynamics/compiler/*` (AST, ClauseSpec, ClauseBuilder, emitters).
  - Deleted old `lib/ecto_shorts/query_builder/dynamics/expression/**` files (including `clause_adapter.ex` and `clause_emitter.ex`).
- Renamed/moved Postgres clause spec groups:
  - `EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.*` → `EctoShorts.QueryBuilder.Dynamics.Postgres.*`
  - Moved to `lib/ecto_shorts/query_builder/dynamics/postgres/**` and updated references.
- Updated `lib/ecto_shorts/query_builder/dynamics.ex` aliases to the new module locations.
- Updated spec/compiler-focused tests to use `Dynamics.Compiler` and `Dynamics.Postgres` namespaces; removed any remaining references to `ClauseEmitter`/`ClauseAdapter`.

