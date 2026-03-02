# Restructure source/query params to :from shape

---
Status: accepted
Date: 2026-03-01
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

The `convert_params_to_filter/3` function in `EctoShorts.CommonFilters` accepts an inline query-builder payload shaped as `%{source: Post, query: %{id: 1}}`. The `:source` key identifies the schema or table, and the `:query` key wraps filter params. This same shape appears in nested contexts: `:exists`, `:all`/`:any`, `:with_cte`, and join `:subquery` payloads.

The current shape has two problems. First, `:source` collides with the join system's `:source` key (which identifies the join target), making subquery join payloads confusing: `source: [source: User, query: [...]]`. Second, the `:query` wrapper adds an extra nesting level that does not carry its weight - it separates filters from the source when they naturally belong together. The question is: how should we reshape this payload so it maps cleanly to JSON, avoids naming collisions, and reduces nesting?

## Decision Drivers

* The payload must map well to JSON for HTTP request bodies where the entire query description comes from data.
* The key naming must not collide with the join `:source` key, which is a different concept on a different code path.
* The shape should reduce nesting by placing filter keys alongside the source identifier instead of wrapping them in a separate `:query` map.
* This is a library with a public API, so the change must be deliberate and documented as a breaking change.

## Considered Options

* **Option A: `%{from: %{query: X, ...filters}}`** - a single `:from` key wrapping a map where `:query` identifies the source and all other keys are filters.
* **Option B: `%{from: %{source: X, ...filters}}`** - same structure but using `:source` inside `:from`. Rejected because `:source` still collides with the join key conceptually, even though the code paths differ.
* **Option C: keep the old shape** - no change. Rejected because the naming collision and extra nesting remain.

## Decision Outcome

Chosen option: "Option A: `%{from: %{query: X, ...filters}}`", because it eliminates the naming collision with the join `:source` key, reduces nesting by removing the separate `:query` wrapper, and maps cleanly to JSON. The `:query` key inside `:from` is required at the top-level entry point and raises `ArgumentError` if missing. In nested contexts (`:all`, `:any`, `:exists`, `:with_cte`, join `:subquery`) it falls back to the parent source when omitted.

### Consequences

Good, because the payload is flatter and easier to construct from JSON data. Good, because there is no naming collision between the `:from` inner `:query` key and the join `:source` key. Good, because the shape is consistent across all five contexts.

Bad, because this is a breaking change to the public API. All callers using the old `%{source: X, query: %{...}}` shape must update. Bad, because the `:query` key inside `:from` now means "the source to query against" rather than "filter params", which is a semantic shift that must be documented clearly.

## Validation

Run the full test suite to confirm the new shape is accepted and the old shape is rejected:

    mix test --seed 0 --trace

Search the codebase for any remaining references to the old pattern:

    grep -rn "source:.*query:" lib/ test/ guides/

The search should return zero matches outside of the join `:source` key usage (which is unchanged).

Review the moduledoc of `EctoShorts.CommonFilters` to confirm all examples use the new `:from` shape.

## Pros and Cons of the Options

### Option A: `%{from: %{query: X, ...filters}}`

Replaces `:source` and `:query` with a single `:from` key. The inner `:query` key identifies the source. All other keys are filters.

Good, because it eliminates the `:source` naming collision with joins.
Good, because it reduces nesting by one level (no separate `:query` wrapper for filters).
Good, because the shape maps directly to JSON without ambiguity.
Bad, because it is a breaking change requiring migration of all callers.

### Option B: `%{from: %{source: X, ...filters}}`

Same structure as Option A but keeps `:source` as the inner key name.

Good, because the inner key name is familiar from the old shape.
Bad, because `:source` still appears in two conceptually different contexts (inside `:from` and as a join target key), which was the original confusion.

### Option C: keep the old shape

No change to the API.

Good, because no migration effort.
Bad, because the naming collision and extra nesting remain.
Bad, because JSON payloads remain awkward with the nested `:query` wrapper.

## More Information

The join `:source` key (used in `%{join: [schema: [source: User, ...]]}`) is unchanged by this decision. It identifies the join target and is popped by `reduce_join/5` in `EctoShorts.CommonFilters.Join`. The two uses of "source" were on different code paths but shared the same key name, which caused confusion in subquery join payloads.

Revisit this decision if the library adds support for multiple `:from` sources (composite queries) or if the `:query` key inside `:from` causes confusion with `Ecto.Query` structs.
