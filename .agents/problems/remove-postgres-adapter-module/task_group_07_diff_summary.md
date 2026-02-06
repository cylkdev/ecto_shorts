# Task Group 07 Diff Summary

## Summary
Removed `@after_compile`/`__after_compile__/2` and `Module.create/3` from the Dynamics compiler flow. `Compiled` modules are now explicit `defmodule`s in `Dynamics.Postgres`, and `Dynamics.Compiler.__using__/1` emits `dynamic_field_expr/3` clauses directly into those modules.

## Key changes
- `EctoShorts.QueryBuilder.Dynamics.Compiler`:
  - Removed the adapter behaviour/callback surface and all compile callback logic.
  - `__using__/1` now takes `specs:` and `max_positional_bindings:` and injects:
    - `import Ecto.Query` / `require Ecto.Query`
    - generated `def dynamic_field_expr/3` clauses (from the spec module + binding patterns).
- `EctoShorts.QueryBuilder.Dynamics.Postgres`:
  - Defines `Compiled.Common`, `Compiled.Scalar`, `Compiled.Array` explicitly via `defmodule`.
  - Each compiled module calls `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: ...`.
- Tests:
  - Replaced the old “adapter compilation” test (which depended on `@after_compile`) with a `__using__/1` test that compiles a module using the compiler and asserts `dynamic_field_expr/3` works and respects `max_positional_bindings`.

