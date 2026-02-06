# Task Group 01 Plan — Remove QueryBuilder adapter layer

## Spec (expected behavior)
- `EctoShorts.QueryBuilder.build_query/6` keeps its current contract and returns an `Ecto.Query.t()`.
- `build_query/6` applies the same dispatch rules that currently live in `EctoShorts.QueryBuilder.Adapters.Postgres`:
  - `:join` uses `EctoShorts.QueryBuilder.Stages.Joins.build/5`
  - `:select` and `:select_merge` use `EctoShorts.QueryBuilder.Stages.Selects.build/5`
  - otherwise use `EctoShorts.QueryBuilder.Stages.Filters.build/6`

## Inputs / Outputs / Invariants
- Inputs: `source`, `query`, `binding_selector`, `current_filter`, `args`, `opts`.
- Output: updated `Ecto.Query.t()`.
- Invariant: dispatch is determined only by `current_filter` and mirrors the prior adapter behavior.

## Edge cases / Failure modes
- Ensure `:select` and `:select_merge` still route to `Selects.build/5` and do not fall through to `Filters.build/6`.

## Files / Modules touched (bounded)
- `lib/ecto_shorts/query_builder.ex`
- `lib/ecto_shorts/query_builder/adapters/postgres.ex` (deleted)
- `lib/ecto_shorts/query_builder/adapter.ex` (deleted)
- `test/...` (new test module for `EctoShorts.QueryBuilder`)
- `.agents/adr/0001-remove-query-builder-adapter-layer.md` (ADR record)

## Tests to add/update
- Add `test/ecto_shorts/query_builder/query_builder_test.exs` that:
  - Asserts default dispatch applies a representative pagination filter (e.g. `:limit`) correctly.

## Acceptance criteria
- No runtime references remain to `EctoShorts.QueryBuilder.Adapters.Postgres`.
- All tests pass and formatting/compilation succeed.
- No query behavior changes beyond module location refactor.
