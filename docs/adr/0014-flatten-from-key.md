# Flatten the :from key to hold the source directly

---
Status: accepted
Date: 2026-03-02
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

The `convert_params_to_filter/3` function in `EctoShorts.CommonFilters` accepts an inline query-builder payload shaped as `%{from: %{query: Post, id: 1}}`. The `:from` key wraps a map where `:query` identifies the source and all other keys are filters. This same shape appears in nested contexts: `:exists`, `:all`/`:any`, `:with_cte`, and join `:subquery` payloads.

The wrapper adds an extra nesting level that does not carry its weight. The `:query` key inside `:from` is required but only serves to separate the source from the filters. The entire `:from` map could be replaced by a flat key where `:from` holds the source directly and sibling keys are filters.

## Decision Drivers

- Reduce nesting by removing the inner `:query` key.
- The payload must map well to JSON for HTTP request bodies.
- The shape must be consistent across all contexts (top-level, nested subquery builders, Source).
- This is a library with a public API, so the change must be deliberate and documented as a breaking change.

## Considered Options

- **Option A: Flatten `:from` to hold the source directly** - `:from` becomes a flat key whose value is the source (schema module, table string, tuple, or query). Sibling keys are filters: `%{from: Post, id: 1}`.
- **Option B: Keep the current shape** - no change. The wrapper remains.

## Decision Outcome

Chosen option: "Option A: Flatten `:from` to hold the source directly", because it removes one level of nesting, eliminates the inner `:query` key, and simplifies the implementation by removing `split_query_source/1`, `extract_from_params/2`, and the merge step.

### Consequences

Good, because the payload is flatter. `%{from: Post, id: 1}` instead of `%{from: %{query: Post, id: 1}}`.

Good, because the implementation is simpler. The top-level handler pops `:from` and gets the source directly instead of extracting it from a nested map. No merge step needed.

Good, because nested subquery builders (`:all`, `:any`, `:exists`, `:with_cte`, join `:subquery`) use the same flat shape, removing duplicated extraction logic in `dynamics.ex`, `with_cte.ex`, and `join.ex`.

Good, because `Source` pops the `source_key` directly from top-level params instead of nesting it inside `:from`.

Bad, because this is a breaking change to the public API. All callers using the old `%{from: %{query: X, ...}}` shape must update.

## Validation

Run the full test suite to confirm the new shape is accepted:

    mix test --seed 0 --trace

Search the codebase for any remaining references to the old pattern:

    grep -rn "from:.*query:" lib/ test/ guides/

The search should return zero matches outside of Ecto's own `from` macro usage.

## Pros and Cons of the Options

### Option A: Flatten :from to hold the source directly

Good, because it removes one level of nesting.
Good, because it eliminates the inner `:query` key and the `split_query_source/1` helper.
Good, because the merge step in `convert_params_to_filter/3` is no longer needed.
Bad, because it is a breaking change requiring migration of all callers.

### Option B: Keep the current shape

Good, because no migration effort.
Bad, because the extra nesting and inner `:query` key remain.
Bad, because `split_query_source/1`, `extract_from_params/2`, and the merge step remain.

## More Information

This decision supersedes ADR 0010 ("Restructure source/query params to :from shape"). ADR 0010 introduced the `%{from: %{query: X, ...}}` shape to resolve a naming collision with the join `:source` key. This decision keeps the `:from` key name but flattens its value from a map to a direct source reference.

Files changed:

- `lib/ecto_shorts/common_filters.ex` - flatten `:from` handling, remove `split_query_source/1`, simplify Source clause, remove dead helpers
- `lib/ecto_shorts/dynamics.ex` - simplify `build_helper_expr_subquery`, remove `extract_from_params/2`
- `lib/ecto_shorts/common_filters/with_cte.ex` - simplify `params_to_query`
- `lib/ecto_shorts/common_filters/join.ex` - simplify subquery source extraction
- `lib/ecto_shorts/schemaless_query.ex` - update docs
- `guides/WORKED_EXAMPLES.md` - update examples
- Tests updated to use the new flat shape
