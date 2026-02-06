# Remove `kind` + emitter indirection from `QueryBuilder.Dynamics.Compiler`

## Status
Accepted

## Context
`EctoShorts.QueryBuilder.Dynamics.Compiler` currently exposes a compile-time adapter contract that includes:

- a `kind` argument threaded through `clause_specs/5` and stored on each `%ClauseSpec{}`,
- a configurable `emitter_module/0` that is responsible for turning specs into quoted `def` clauses.

In practice:
- `kind` is not used for clause generation; it is mostly a tag carried through the system.
- the emitter module is effectively fixed to emitting `dynamic_field_expr/3` clauses and ignores `kind`.

This adds surface area without providing meaningful value for the library’s current scope (single shipped adapter, fixed function name).

## Decision
- Remove `kind` from the compiler adapter public API:
  - no `kind` argument in compiler callbacks,
  - no `:kind` field in `%ClauseSpec{}`.
- Remove the dedicated emitter module and emitter callback:
  - `Dynamics.Compiler` generates `def dynamic_field_expr/3` clauses directly from clause specs,
  - adapters no longer provide `emitter_module/0`.
- Remove the public notion of spec grouping as threaded callback “kinds”:
  - there is no `kind` argument and no `:kind` on clause specs,
  - the existing scalar/array/common dispatch remains internal by compiling `Adapter.Compiled.{Common,Scalar,Array}` modules and selecting specs based on the `context` (compiled module) argument.
- Merge the compile-time adapter hook and compilation logic into `EctoShorts.QueryBuilder.Dynamics.Compiler`:
  - keep the `use EctoShorts.QueryBuilder.Dynamics.Compiler` macro,
  - move the previous `ClauseBuilder.__after_compile__/2` responsibilities into `Dynamics.Compiler` to reduce internal module sprawl.
- Keep the existing “grouping” concept (`common` / `scalar` / `array`) as an internal compiler concern, but do not expose it as a threaded `kind` argument on the callback or specs.

## Consequences
### Easier
- Smaller adapter surface area (fewer callbacks and less repeated plumbing).
- Clause specs represent only the clause head/body/guard needed to generate `dynamic_field_expr/3`.
- Less confusing terminology (no “kind” tag that doesn’t affect output).
- Fewer internal modules; the compiler entrypoint is the single place to look for compilation behavior.

### Harder / Tradeoffs
- This is a breaking change for any external consumer implementing the compiler adapter contract.
- Tests that asserted `kind` propagation (directly or indirectly) must be updated/removed.
