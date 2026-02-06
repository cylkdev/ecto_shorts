# Task Group 08 Behavior Summary

## What stays the same
- Query semantics produced by `EctoShorts.QueryBuilder.Dynamics.convert_to_dynamic/3` are unchanged (same boolean reduction, same schema `query_fields` validation + warnings, same expression shapes passed to expression builders).

## What changed (internals / boundaries)
- Compiled clause ownership is now predictable per expression module:
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.CommonExpr.Compiled`
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.ScalarExpr.Compiled`
  - `EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Compiled`
- The expression module author no longer writes `dynamic_field_expr/3` boilerplate or `@compile` warning suppressions; `use EctoShorts.QueryBuilder.Dynamics.Compiler` generates:
  - `X.Compiled` (pattern-matching `dynamic_field_expr/3` clauses)
  - `X.dynamic_field_expr/3` delegating into `X.Compiled`
- Adapter separation is tightened:
  - `EctoShorts.QueryBuilder.Dynamics` no longer owns the “common operator keys” list and no longer performs Scalar-vs-Array routing.
  - `EctoShorts.QueryBuilder.Dynamics.Postgres` owns:
    - `operators/0` (adapter-level operator keys)
    - `build_dynamic_expression/4` (routes to CommonExpr/ScalarExpr/ArrayExpr using `source` + `key`)

## Edge cases / failure modes
- If an unexpected `{key, expr}` shape reaches the adapter expression builders, the call will raise due to no matching `dynamic_field_expr/3` clause (unchanged behavior, just routed through `Postgres.build_dynamic_expression/4`).

