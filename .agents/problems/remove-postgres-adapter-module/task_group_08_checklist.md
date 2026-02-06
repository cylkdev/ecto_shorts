# Task Group 08 Checklist

- [x] ADR-0008 approved (predictable `<Caller>.Compiled` naming; clause specs live in a separate `X.Specs` module).
- [x] Update `lib/ecto_shorts/query_builder/dynamics/compiler.ex` `__using__/1` to:
  - define `defmodule <Caller>.Compiled` containing `dynamic_field_expr/3` clauses
  - define `dynamic_field_expr/3` in the caller module, delegating to `<Caller>.Compiled.dynamic_field_expr/3`
- [x] Update `lib/ecto_shorts/query_builder/dynamics.ex` to:
  - use `Postgres.operators/0` to detect adapter operator keys
  - delegate predicate building to `Postgres.build_dynamic_expression/4`
  - keep existing boolean-group reduction and query_fields warning behavior
- [x] Update `lib/ecto_shorts/query_builder/dynamics/postgres.ex` to export only:
  - `operators/0`
  - `build_dynamic_expression/4`
- [x] Migrate Postgres clause specs into `...Postgres.{CommonExpr,ScalarExpr,ArrayExpr}.Specs` modules (one module per file).
- [x] Update Postgres expression modules to `use ...Compiler` (no manual `dynamic_field_expr/3`, no `@compile` directives).
- [x] Update/replace tests that assert the old compiled module path.
- [x] Run `mix format`, `mix compile`, and focused `mix test`.
