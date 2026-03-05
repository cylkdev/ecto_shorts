# 0002 - Thin Adapter Refactor

Move all predicate routing, boolean handling, schema validation, payload resolution, and predicate merging from `Filter` and `Postgres` into `Dynamics`, making the adapter a pure leaf.

## Status

In progress.

## Problem

Three separate routing passes exist for the same data today.

1. `Filter.build/6` checks: boolean op? custom filter? schema field?
2. `Postgres.append_predicates` checks: operator? boolean? schema field?
3. `Postgres.build_dynamic/4` checks: operator? array field? scalar?

The operator list `[:ids, :before, :after, :start_date, :end_date, :exists]` is declared in both `Filter` (`@custom_filters`) and `Postgres` (`@operators`). Schema field validation happens in both `Filter.build/6` (line 110) and `Postgres.append_predicates` (line 316-319). Boolean handling lives in `Filter.build/6`, `Postgres.expand_and_reduce`, and `Having`'s `reduce_having/*` loop.

## Solution

One routing pass, in `Dynamics`. The adapter is a pure leaf.

## Steps

### Step 1 - Add routing logic to `Dynamics`

Move from `Postgres` to `Dynamics`:

- `append_predicates/4` (generalized, calls `adapter.build_dynamic/4`)
- `expand_and_reduce/5`
- `merge_predicate/3`
- `normalize_entries/2`
- `source_has_schema?/1`

Move from `Filter` to `Dynamics`:

- `resolve_exists_payload/3`
- `resolve_quantifier_payload/4`
- `resolve_quantifier_inner/4`
- `put_default_select/2`

`convert_to_dynamic/4` calls `append_predicates/4` instead of `adapter.convert_to_dynamic/3`.

### Step 2 - Shrink `Postgres` adapter

Remove: `convert_to_dynamic/3`, `append_predicates/4` and all clauses, `expand_and_reduce/5`, `merge_predicate/3`, `source_has_schema?/1`, `@boolean_operators`, `normalize_entries/2`.

Keep: `operators/0`, `operator?/1`, `build_dynamic/4`, `build_dynamic_or_warn/4`, `reduce_entries/4`, `@operators`, `@datetime_operators`.

### Step 3 - Remove `convert_to_dynamic/3` callback from `EctoShorts.Dynamic`

### Step 4 - Simplify `Filter`

Remove: boolean `build/6` clause (lines 49-60), custom filter `build/6` clause (lines 62-77), `resolve_exists_payload/*`, `resolve_quantifier_payload/*`, `resolve_quantifier_inner/*`, `build_field/7`, `put_default_select/2`.

The field clause becomes:

```elixir
def build(schema_source, filter, query, binding_selector, {key, value}, opts) do
  dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, {key, value}, opts)
  apply_where_expr(filter, query, dyn)
end
```

### Step 5 - Simplify `Having`

Remove all `reduce_having/*` unwrapping clauses. Replace with:

```elixir
def build(schema_source, filter_op, query, binding_selector, params, opts)
    when filter_op in [:having, :or_having] do
  dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, params, opts)
  reduce_having_expr(filter_op, query, binding_selector, dyn)
end
```

### Step 6 - Update test support adapters

Remove `convert_to_dynamic/3` from `TestDynamicExpressionAdapter` and `TestPayloadProbeAdapter`.

### Step 7 - Rewrite 2 dynamics tests

Tests at lines 108-136 of `dynamics_test.exs` that use `assert_received` are rewritten using the real Postgres adapter and `assert_dynamic`, since payload resolution now happens in `Dynamics` before the adapter.

### Step 8 - Run full test suite
