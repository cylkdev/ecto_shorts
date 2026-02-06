# Task Group 01 Behavior Summary

## Public API behavior
- `EctoShorts.QueryBuilder.build_query/6` still accepts `(source, query, binding_selector, current_filter, args, opts)` and returns an `Ecto.Query.t()`.
- Stage routing behavior is unchanged (same routing that previously lived in the Postgres adapter):
  - `current_filter == :join` applies join logic via `EctoShorts.QueryBuilder.Stages.Joins`.
  - `current_filter in [:select, :select_merge]` applies select logic via `EctoShorts.QueryBuilder.Stages.Selects`.
  - All other filters apply via `EctoShorts.QueryBuilder.Stages.Filters`.

## Removed modules
- `EctoShorts.QueryBuilder.Adapters.Postgres` no longer exists (including `filters/0`).
- `EctoShorts.QueryBuilder.Adapter` no longer exists.

## Edge cases
- `:select_merge` routing is preserved exactly as before (it is still routed to `Stages.Selects`).

