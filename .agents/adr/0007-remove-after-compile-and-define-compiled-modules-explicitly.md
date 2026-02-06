# Remove `@after_compile` and define `Compiled` modules explicitly with `defmodule`

## Status
Accepted

## Context
`EctoShorts.QueryBuilder.Dynamics.Postgres` currently relies on compile callbacks and runtime module creation to generate:
- `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Common`
- `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Scalar`
- `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.Array`

The current mechanism uses `@after_compile` plus `Module.create/3` (and previously included additional adapter/behaviour indirection).

This has proven harder to reason about than necessary for this library’s scope. The desired outcome is “extremely simple and explicit”: compiled modules should be normal modules defined in code via `defmodule`, without `@after_compile` or `Module.create/3`.

## Decision
- Remove `@after_compile` and `__after_compile__/2` from `EctoShorts.QueryBuilder.Dynamics.Compiler`.
- Remove use of `Module.create/3` for generating compiled modules.
- Define `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.{Common,Scalar,Array}` explicitly in `lib/ecto_shorts/query_builder/dynamics/postgres.ex` using `defmodule`.
- Keep the “spec-driven” approach for clause authoring, but generate the function clauses via `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: ...` inside each `Compiled.*` module body:
  - `Dynamics.Compiler.__using__/1` calls the already-compiled spec group modules (`Dynamics.Postgres.Specs.*`) and emits `def dynamic_field_expr/3` clauses.
- Keep the earlier TG06 simplifications:
  - no `kind` argument anywhere and no `:kind` field on clause specs
  - no emitter module indirection; clause emission is fixed to `dynamic_field_expr/3`

## Consequences
### Easier
- No compile callback indirection (`@after_compile`) and no runtime module creation.
- `Compiled` modules are explicit and searchable in source.
- The compiler becomes a small “macro utility” rather than a module-generation engine.

### Harder / Tradeoffs
- Removes the “adapter” shape from `Dynamics.Compiler` (tests that validated the adapter contract need to be replaced).
- Slightly more code in `postgres.ex` because the `Compiled.*` modules are defined explicitly.
