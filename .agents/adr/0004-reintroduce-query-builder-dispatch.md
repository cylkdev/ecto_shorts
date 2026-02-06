# Reintroduce `EctoShorts.QueryBuilder` as Dispatch Module

## Status
Proposed

## Context
ADR-0002 removed the top-level `EctoShorts.QueryBuilder` module and moved its dispatch logic into `EctoShorts.CommonFilters` to simplify the public API surface.

After that change, the query-filtering domain still needs a coherent internal home for:
- dispatching a single filter step to the correct implementation (join/select/filter),
- keeping `EctoShorts.CommonFilters` focused on parameter normalization + reduction rather than stage routing,
- reorganizing the remaining `lib/ecto_shorts/query_builder/**` code (bindings + dynamic-expression compilation) into a connected namespace.

## Decision
- Reintroduce a top-level `EctoShorts.QueryBuilder` module that provides `build_query/6` and owns dispatch.
- Update `EctoShorts.CommonFilters` to call `EctoShorts.QueryBuilder.build_query/6` (instead of dispatching to stages directly).
- Move stage modules under `EctoShorts.QueryBuilder.*` (`Joins`, `Selects`, `Filters`) so stage routing is local to the QueryBuilder domain.

## Consequences
### Easier
- Clear internal boundary: `CommonFilters` reduces params; `QueryBuilder` routes a single step.
- More coherent on-disk organization for the `query_builder/**` tree.

### Harder / Tradeoffs
- Reintroduces a top-level module name that was previously removed (potentially confusing for users if it is considered public).
- May need a follow-up ADR to clarify whether `EctoShorts.QueryBuilder` is treated as public API or internal-only.

