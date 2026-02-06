# Reorganize Filter Internals (Stages / Bindings / Dynamics)

## Status
Proposed

## Context
`EctoShorts.QueryBuilder` was removed previously and dispatch moved into `EctoShorts.CommonFilters`.

However, multiple internal modules remain under the `EctoShorts.QueryBuilder.*` namespace:
- `EctoShorts.QueryBuilder.Stages.*` (invoked by `EctoShorts.CommonFilters`)
- `EctoShorts.QueryBuilder.BindingHelpers` (used by stages + dynamic-expression tooling)
- `EctoShorts.QueryBuilder.Dynamics` and `lib/ecto_shorts/query_builder/dynamics/**` (dynamic-expression compilation)

This naming/layout is now disconnected from the public/primary entrypoint (`CommonFilters`) and makes the domain harder to navigate.

## Decision
Reorganize the internal modules to reflect the domain boundaries:
- Introduce/reintroduce `EctoShorts.QueryBuilder` to own dispatch (`build_query/6`), so `EctoShorts.CommonFilters` can focus on param normalization/reduction.
- Move stage modules under `EctoShorts.QueryBuilder.*` (`Joins`, `Selects`, `Filters`).
- Keep binding-pattern generation under QueryBuilder and rename `BindingHelpers` to `EctoShorts.QueryBuilder.Bindings`.
- Reorganize the dynamic-expression compilation subtree so it is connected and navigable:
  - Keep `EctoShorts.QueryBuilder.Dynamics` as the runtime entrypoint.
  - Rename `Dynamics.Expressions.Postgres` → `Dynamics.Postgres` (avoid plural “Expressions”).
  - Rename `Dynamics.Expression` → `Dynamics.Compiler` and merge ClauseAdapter into it.
  - Remove the old `ClauseEmitter` behaviour module (emitters are validated by exported `quoted_def/6` only).

## Consequences
### Easier
- Better discoverability: stage and dynamics code sits under the domain that calls it.
- A clearer separation of responsibilities: `CommonFilters` reduces params; `QueryBuilder` routes a single step; expressions compile dynamics.

### Harder / Tradeoffs
- Module renames are potentially breaking for consumers that referenced these internal modules directly.
