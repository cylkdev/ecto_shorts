# Task Group 03 Checklist

- [ ] Draft ADR 0004 and get explicit approval.
- [ ] Update ADR 0003 to match and get explicit approval.
- [ ] Add `lib/ecto_shorts/query_builder.ex` with `EctoShorts.QueryBuilder.build_query/6`.
- [ ] Update `lib/ecto_shorts/common_filters.ex` to call `EctoShorts.QueryBuilder.build_query/6`.
- [ ] Move/rename `Joins` stage module under `EctoShorts.QueryBuilder.Joins`.
- [ ] Move/rename `Selects` stage module under `EctoShorts.QueryBuilder.Selects`.
- [ ] Move/rename `Filters` stage module under `EctoShorts.QueryBuilder.Filters`.
- [ ] Update `EctoShorts.QueryBuilder` to call the renamed stage modules.
- [ ] Run `mix format`.
- [ ] Run `mix compile`.
- [ ] Run `mix test test/ecto_shorts/common_filters_test.exs`.
