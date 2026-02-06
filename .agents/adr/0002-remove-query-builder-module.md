# Remove `EctoShorts.QueryBuilder` module

## Status
Accepted

## Context
The library currently exposes `EctoShorts.QueryBuilder.build_query/6` as a public entrypoint for applying a single filter step to an `Ecto.Query`.

However, the only internal runtime call site is `EctoShorts.CommonFilters` (via `apply_query_builder/6`), and the remaining modules under `EctoShorts.QueryBuilder.*` (stages, dynamics, etc.) already provide the real behavior.

Keeping `EctoShorts.QueryBuilder` as a separate public module adds an extra public API surface and an extra indirection without providing meaningful value for this library’s intended usage (which is primarily through `EctoShorts.CommonFilters.convert_params_to_filter/3`).

## Decision
- Remove the `EctoShorts.QueryBuilder` module.
- Inline its dispatch logic into `EctoShorts.CommonFilters` (the only internal call site).

## Consequences
### Easier
- Smaller public API surface.
- Fewer modules to understand for query building.

### Harder / Tradeoffs
- Breaking change for external consumers calling `EctoShorts.QueryBuilder.build_query/6` directly.
