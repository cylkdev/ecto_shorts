# Task Group 02 Behavior Summary

## Primary behavior
- `EctoShorts.CommonFilters.convert_params_to_filter/3` continues to build queries the same way.
- The stage routing behavior is unchanged, but now happens inside `EctoShorts.CommonFilters`:
  - `:join` routes to `EctoShorts.QueryBuilder.Stages.Joins`
  - `:select` / `:select_merge` route to `EctoShorts.QueryBuilder.Stages.Selects`
  - all other filters route to `EctoShorts.QueryBuilder.Stages.Filters`

## Breaking change
- `EctoShorts.QueryBuilder` (and `build_query/6`) no longer exists. External callers must use `EctoShorts.CommonFilters.convert_params_to_filter/3` (or call stages directly if they were already doing that).

