# Task Group 03 Plan — Introduce top-level `EctoShorts.QueryBuilder` and move stages under it

## Spec (expected behavior)
- No behavior changes. The query AST produced by `EctoShorts.CommonFilters.convert_params_to_filter/3` remains the same.
- `EctoShorts.CommonFilters` no longer dispatches directly to join/select/filter stage modules.
- A new module `EctoShorts.QueryBuilder` owns dispatch via `build_query/6` and is called by `EctoShorts.CommonFilters`.
- Stage modules are moved/renamed under `EctoShorts.QueryBuilder.*`:
  - `EctoShorts.QueryBuilder.Stages.Joins` → `EctoShorts.QueryBuilder.Joins`
  - `EctoShorts.QueryBuilder.Stages.Filters` → `EctoShorts.QueryBuilder.Filters`
  - `EctoShorts.QueryBuilder.Stages.Selects` → `EctoShorts.QueryBuilder.Selects`

## Inputs / Outputs / Invariants
- Inputs: existing filter parameters + options passed into `EctoShorts.CommonFilters`.
- Output: identical `Ecto.Query.t()` results.
- Invariant: stage function signatures and logic remain unchanged; only their module names/paths change.

## Edge cases / Failure modes
- Ensure `:join` continues to call the join stage with the same args (currently `build/4`).
- Ensure `:select` / `:select_merge` continue to route to the selects stage (`build/5`).
- Ensure non-schema-key warning behavior remains unchanged.

## Files/modules affected (bounded)
- `lib/ecto_shorts/common_filters.ex` (update to call `EctoShorts.QueryBuilder.build_query/6`)
- `lib/ecto_shorts/query_builder.ex` (new)
- `lib/ecto_shorts/query_builder/stages/filters.ex` (move + module rename)
- `lib/ecto_shorts/query_builder/stages/joins.ex` (move + module rename)
- `lib/ecto_shorts/query_builder/stages/selects.ex` (move + module rename)
- `.agents/adr/0004-reintroduce-query-builder-dispatch.md` (ADR; proposed)
- `.agents/adr/0003-reorganize-filter-internals.md` (ADR; proposed; updated)

## Tests
- No new tests planned; rely on `test/ecto_shorts/common_filters_test.exs` as contract coverage.

## Checklist
- [ ] Draft ADR 0004 (reintroduce `EctoShorts.QueryBuilder` dispatch) and get explicit approval.
- [ ] Update ADR 0003 to match the new structure (stages under `EctoShorts.QueryBuilder.*`) and get explicit approval.
- [ ] Create `lib/ecto_shorts/query_builder.ex` defining `EctoShorts.QueryBuilder.build_query/6`.
- [ ] Update `lib/ecto_shorts/common_filters.ex` to call `EctoShorts.QueryBuilder.build_query/6` (move the `case` dispatch out of `CommonFilters`).
- [ ] Move + rename Joins stage to `EctoShorts.QueryBuilder.Joins`.
- [ ] Move + rename Selects stage to `EctoShorts.QueryBuilder.Selects`.
- [ ] Move + rename Filters stage to `EctoShorts.QueryBuilder.Filters`.
- [ ] Update `EctoShorts.QueryBuilder` to call the renamed stage modules.
- [ ] `mix format`
- [ ] `mix compile`
- [ ] `mix test test/ecto_shorts/common_filters_test.exs`
