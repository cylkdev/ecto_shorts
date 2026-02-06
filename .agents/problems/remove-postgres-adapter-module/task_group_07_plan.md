# Task Group 07 Plan — Remove `@after_compile` and define `Compiled` modules explicitly

## Goal (one sentence)
Eliminate `@after_compile`/`Module.create` from the Dynamics compiler and define `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.{Common,Scalar,Array}` as explicit `defmodule`s in source, generated via a single explicit macro call per module.

## Understanding (what you want)
- No `__after_compile__/2` anywhere for this feature.
- No runtime module creation (`Module.create/3`).
- Keep things extremely simple and explicit:
  - `Compiled` modules should be visible in `postgres.ex` as `defmodule` blocks.
  - Clause emission should be fixed to `dynamic_field_expr/3` and owned by the library.
  - `kind` stays fully removed (no `kind` args, no `:kind` in specs).

## Proposed shape (end state)

### `EctoShorts.QueryBuilder.Dynamics.Compiler`
- Becomes a small helper module (no compile callbacks, no runtime module creation):
  - keep `ClauseSpec` validation
  - keep a `clause_ast/1` helper (spec → quoted `def dynamic_field_expr/3`)
  - use **`__using__/1` only** to emit the `dynamic_field_expr/3` clauses directly into the caller module at compile time.
- `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: SpecsModule, max_positional_bindings: 10`:
  - imports/requires `Ecto.Query`
  - builds binding patterns via `BindingHelpers.query_var_and_binding_heads(__CALLER__.module, max_positional_bindings: ...)`
  - calls `SpecsModule.clause_specs(__CALLER__.module, binding_head_ast, target_binding_var, binding_body_asts)`
  - emits `unquote_splicing` of the generated quoted `def dynamic_field_expr/3` clauses
- Remove:
  - `@after_compile` wiring from `__using__/1`
  - `__after_compile__/2` function
  - any usage of `Module.create/3`

### `EctoShorts.QueryBuilder.Dynamics.Postgres`
- Stop being an “adapter” that is compiled after-the-fact.
- Define compiled modules explicitly:
  - `defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Common do ... end`
  - `defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Scalar do ... end`
  - `defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Array do ... end`
- Each module:
  - `use EctoShorts.QueryBuilder.Dynamics.Compiler` once, passing the relevant spec module:
    - `CommonExprs`
    - `ScalarExprs`
    - `ArrayExprs`

### Specs modules
- Keep the current `clause_specs/4` API on:
  - `Dynamics.Postgres.Specs.CommonExprs`
  - `Dynamics.Postgres.Specs.ScalarExprs`
  - `Dynamics.Postgres.Specs.ArrayExprs`
- Ensure clause specs remain “kindless” and validate via `ClauseSpec`.

## Tests to update
- Replace the current “adapter compilation” test (which exists to validate `use Compiler` + `@after_compile`) with a simpler test:
  - the explicit `defmodule …Compiled.*` modules are compiled
  - `dynamic_field_expr/3` exists and returns the expected dynamic for a representative spec
- Keep/adjust the existing ClauseSpec + clause emission tests to match the new API (no `@after_compile`, no `Module.create`).

## Acceptance criteria
- `rg` finds no `@after_compile` / `__after_compile__` / `Module.create(` in `lib/ecto_shorts/query_builder/dynamics/**`.
- `postgres.ex` contains explicit `defmodule …Compiled.Common/Scalar/Array` definitions.
- `mix format` and `mix compile` succeed.
- Focused tests pass:
  - `mix test test/ecto_shorts/common_filters_test.exs`
  - `mix test test/ecto_shorts/query_builder/dynamics/repo_adapter_opts_test.exs`
  - relevant compiler/spec tests

## Checklist
- [ ] ADR-0007 approved (remove `@after_compile`, define `Compiled` modules explicitly).
- [ ] Update `Dynamics.Compiler` to remove compile callbacks and emit clauses via `__using__/1` (no separate macro).
- [ ] Update `Dynamics.Postgres` to define `Compiled.Common/Scalar/Array` via explicit `defmodule` blocks and `use Dynamics.Compiler, specs: ...`.
- [ ] Update/remove tests that validate `@after_compile` behavior; replace with explicit compiled-module tests.
- [ ] `rg` verification: no `@after_compile`, `__after_compile__`, or `Module.create` remain in Dynamics.
- [ ] Run `mix format`, `mix compile`, and focused `mix test`.
