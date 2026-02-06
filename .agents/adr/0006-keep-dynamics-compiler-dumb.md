# Keep `QueryBuilder.Dynamics.Compiler` as contract-only

## Status
Proposed

## Context
Historically, the “compiler entrypoint” module was primarily a contract/macro boundary, and the compile-time module generation lived in a dedicated implementation module (for example, `Dynamics.Expression.ClauseBuilder.__after_compile__/2` in the pre-refactor layout).

During TG06, the compilation logic (module generation + clause emission helpers) was moved into `EctoShorts.QueryBuilder.Dynamics.Compiler` itself. This makes the compiler module a “do-everything” module, which is harder to reason about and contradicts the earlier “dumb compiler” architecture goal.

## Decision
- Keep `EctoShorts.QueryBuilder.Dynamics.Compiler` as:
  - the adapter behaviour contract, and
  - the `use` macro entrypoint.
- Move compile-time module generation into a dedicated internal module (for example: `EctoShorts.QueryBuilder.Dynamics.Compiler.Builder`) that owns:
  - `__after_compile__/2`,
  - `Module.create/3` calls for `Adapter.Compiled.{Common,Scalar,Array}`,
  - clause emission helper(s) for `dynamic_field_expr/3`.

## Consequences
### Easier
- Clear separation: “contract” vs “implementation”.
- Less cognitive load when reading `Dynamics.Compiler`.
- Implementation code can evolve without expanding the adapter contract module.

### Harder / Tradeoffs
- Adds one internal module back under `dynamics/compiler/**`.
- Minor churn in internal test modules/docs that currently call compiler helpers directly.

