# Task Group 01 Diff Summary

## Goal
Remove the `EctoShorts.QueryBuilder` adapter layer and inline the Postgres dispatch where it is used.

## Changes
- `lib/ecto_shorts/query_builder.ex`
  - Removed adapter delegation and inlined dispatch to stages:
    - `:join` → `EctoShorts.QueryBuilder.Stages.Joins.build/4`
    - `:select` / `:select_merge` → `EctoShorts.QueryBuilder.Stages.Selects.build/5`
    - all others → `EctoShorts.QueryBuilder.Stages.Filters.build/6`
  - Updated moduledoc/docstrings to describe stage routing and point DB-specific behavior to `EctoShorts.QueryBuilder.Dynamics`.

- Removed modules
  - Deleted `lib/ecto_shorts/query_builder/adapters/postgres.ex` (`EctoShorts.QueryBuilder.Adapters.Postgres`).
  - Deleted `lib/ecto_shorts/query_builder/adapter.ex` (`EctoShorts.QueryBuilder.Adapter`).
  - Removed empty directory `lib/ecto_shorts/query_builder/adapters/`.

- Tests
  - Added `test/ecto_shorts/query_builder/query_builder_test.exs` to cover default dispatch for a representative pagination filter (`:limit`) using `assert_query/2`.

- ADR
  - `.agents/adr/0001-remove-query-builder-adapter-layer.md` status set to `Accepted`.

