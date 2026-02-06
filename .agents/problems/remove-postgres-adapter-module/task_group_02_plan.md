# Task Group 02 Plan — Remove `EctoShorts.QueryBuilder` module

## Spec (expected behavior)
- `EctoShorts.CommonFilters.convert_params_to_filter/3` continues to build queries exactly as before.
- `EctoShorts.QueryBuilder` (public module) no longer exists; the dispatch logic previously in `EctoShorts.QueryBuilder.build_query/6` is moved into `EctoShorts.CommonFilters` where it is used.
- Stage routing remains unchanged:
  - `:join` routes to `EctoShorts.QueryBuilder.Stages.Joins`
  - `:select` / `:select_merge` route to `EctoShorts.QueryBuilder.Stages.Selects`
  - all others route to `EctoShorts.QueryBuilder.Stages.Filters`

## Inputs / Outputs / Invariants
- Inputs: filter params passed to `convert_params_to_filter/3`, plus its existing options.
- Output: an `Ecto.Query.t()` with the same semantics as before.
- Invariant: only module boundaries change; query AST and behavior do not.

## Edge cases / Failure modes
- Named/positional binding dispatch (`{:as, _}` / `{:at, _}`) remains exactly as before because `to_binding_source/3` stays in `EctoShorts.CommonFilters`.
- `:join` keeps the same signature (no `opts` passed) to match `Stages.Joins.build/4`.

## Files / Modules touched (bounded)
- `lib/ecto_shorts/common_filters.ex`
- `lib/ecto_shorts/query_builder.ex` (deleted)
- `mix.exs` (docs grouping)
- `test/ecto_shorts/query_builder/query_builder_test.exs` (deleted) and/or adjust tests to validate via `CommonFilters`.
- `.agents/adr/0002-remove-query-builder-module.md` (ADR record)

## Tests to add/update
- Remove the dedicated `EctoShorts.QueryBuilder` test module.
- Rely on existing `test/ecto_shorts/common_filters_test.exs` for behavior coverage; add a small targeted test only if coverage gaps appear during review.

## Acceptance criteria
- No references remain to `EctoShorts.QueryBuilder`.
- `mix format` and `mix compile` succeed.
- `mix test test/ecto_shorts/common_filters_test.exs` passes.

