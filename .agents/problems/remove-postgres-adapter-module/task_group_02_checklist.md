# Task Group 02 Checklist

- [x] Add ADR record under `.agents/adr/` and get explicit approval.
- [x] Update `lib/ecto_shorts/common_filters.ex` to route to `Stages.Joins/Selects/Filters` directly (no `EctoShorts.QueryBuilder` call).
- [x] Delete `lib/ecto_shorts/query_builder.ex`.
- [x] Delete/update tests that reference `EctoShorts.QueryBuilder`.
- [x] Update `mix.exs` docs grouping to remove `EctoShorts.QueryBuilder`.
- [x] Run `mix format`.
- [x] Run `mix compile`.
- [x] Run `mix test test/ecto_shorts/common_filters_test.exs`.
