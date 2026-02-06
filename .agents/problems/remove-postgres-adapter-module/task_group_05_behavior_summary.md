# Task Group 05 Behavior Summary

## Expected behavior (no functional change)
- `EctoShorts.QueryBuilder.Dynamics` continues to build Ecto `dynamic/2` predicates from filter params the same way as before.
- Repo adapter enforcement (`Ecto.Adapters.Postgres`) behavior is unchanged.

## Structural/API behavior changes
- The compile-time “adapter hook” is now `use EctoShorts.QueryBuilder.Dynamics.Compiler` (instead of `use ...Expression.ClauseAdapter`).
- Emitters no longer implement a `ClauseEmitter` behaviour; emitters are validated by exporting `quoted_def/6`.
- Postgres clause spec groups live under `EctoShorts.QueryBuilder.Dynamics.Postgres.*` (instead of `...Dynamics.Expressions.Postgres.*`).

## Notes / edge cases
- `BindingHelpers` was intentionally not renamed/moved as part of TG05 (that refactor belongs to the QueryBuilder domain re-org work, not this Dynamics compiler/Postgres namespace change).

