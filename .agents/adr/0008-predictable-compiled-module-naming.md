# Predictable `.Compiled` module naming for `Dynamics.Compiler`

## Status
Accepted

## Context
After ADR-0007, the library removed `@after_compile`/`Module.create` and now defines `EctoShorts.QueryBuilder.Dynamics.Postgres.Compiled.{Common,Scalar,Array}` explicitly.

This is simpler than compile callbacks, but it still forces a “special” module (`Dynamics.Postgres`) to be the owner of compiled modules.

The desired outcome is a predictable, local pattern:
- when a module `X` uses the compiler, it should own the compiled output as `X.Compiled`.

Example:
- `EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr` should own `EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Compiled`.

## Decision
- Update `EctoShorts.QueryBuilder.Dynamics.Compiler.__using__/1` so that using it in a caller module `X` defines a nested module `X.Compiled`.
- `X.Compiled` contains the generated `dynamic_field_expr/3` clauses.
- Clause emission remains fixed to `dynamic_field_expr/3` (no emitter module), and `:kind` remains removed.
- Clause specs are provided by a separate, already-compiled module passed explicitly via `specs: SomeSpecsModule`.
  - Convention: for a caller module `X`, the specs module is `X.Specs`.

## Notes
Generating pattern-matching `dynamic_field_expr/3` clauses requires clause specs to be available at compile time.

A runtime `def clause_specs/…` defined in the same module `X` cannot be executed during `__using__/1` expansion (because `X` is not compiled/loaded yet), which is why specs must live in a separate module.

## Consequences
### Easier
- Predictable naming and discoverability (`X.Compiled`).
- Keeps compilation explicit in the caller module (no callbacks).

### Harder / Tradeoffs
- Specs must live in a separate module (typically `X.Specs`) so the compiler can execute `clause_specs/4` during macro expansion.
