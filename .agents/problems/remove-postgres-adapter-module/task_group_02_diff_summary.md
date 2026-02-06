# Task Group 02 Diff Summary

## Goal
Remove the `EctoShorts.QueryBuilder` module and move its dispatch logic to the only call site (`EctoShorts.CommonFilters`).

## Changes
- `lib/ecto_shorts/common_filters.ex`
  - Removed the dependency on `EctoShorts.QueryBuilder.build_query/6`.
  - Inlined the same dispatch routing directly in `apply_query_builder/6`:
    - `:join` → `EctoShorts.QueryBuilder.Stages.Joins.build/4`
    - `:select` / `:select_merge` → `EctoShorts.QueryBuilder.Stages.Selects.build/5`
    - all others → `EctoShorts.QueryBuilder.Stages.Filters.build/6`

- `lib/ecto_shorts/query_builder.ex`
  - Deleted (module removed).

- `mix.exs`
  - Updated ExDoc grouping to no longer reference `EctoShorts.QueryBuilder` (now lists `EctoShorts.QueryBuilder.Dynamics` and `EctoShorts.QueryBuilder.ParamPreprocessor`).

- Tests
  - Deleted `test/ecto_shorts/query_builder/query_builder_test.exs` (no longer applicable after removing the module).

- ADR
  - `.agents/adr/0002-remove-query-builder-module.md` status set to `Accepted`.

