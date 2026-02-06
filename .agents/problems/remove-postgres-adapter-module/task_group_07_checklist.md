# Task Group 07 Checklist

- [x] ADR-0007 approved (remove `@after_compile`, define `Compiled` modules explicitly with `defmodule`).
- [x] Remove `@after_compile` and `__after_compile__/2` from `lib/ecto_shorts/query_builder/dynamics/compiler.ex`.
- [x] Remove all `Module.create/3` usage in `lib/ecto_shorts/query_builder/dynamics/**`.
- [x] Update `lib/ecto_shorts/query_builder/dynamics/compiler.ex` `__using__/1` to emit `dynamic_field_expr/3` clauses given `:specs` + `:max_positional_bindings` options (no separate macro).
- [x] Update `lib/ecto_shorts/query_builder/dynamics/postgres.ex` to define:
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Common`
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Scalar`
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Array`
  using `defmodule` + `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: ...` per module.
- [x] Update tests to remove `@after_compile`-specific assertions and replace with explicit `Compiled.*` module compilation + behavior assertions.
- [x] `rg` confirms no `@after_compile`, `__after_compile__`, or `Module.create(` remain under `lib/ecto_shorts/query_builder/dynamics`.
- [x] Run `mix format`.
- [x] Run `mix compile`.
- [x] Run focused `mix test` for the affected areas.
