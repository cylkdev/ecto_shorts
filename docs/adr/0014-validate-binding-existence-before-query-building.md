# Validate Binding Existence Before Query Building

---
Status: accepted
Date: 2026-03-02
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

When a caller passes a `:bind` parameter with a positional (`:at`) or named (`:as`) binding selector, `EctoShorts.CommonFilters` delegates filter building to the configured dynamic adapter. If the binding does not actually exist in the query, Ecto silently builds an invalid query struct that only raises at execution time. This makes the root cause hard to trace because the error appears far from the invalid input.

The existing code in `create_schema_filter` checked positional bindings against `Config.max_binding_positions()` (a static config value, default 5) but did not check against the query's actual binding count. Named bindings had no existence check at all.

## Decision Drivers

- Fail-fast principle: invalid inputs should be caught as close to the call site as possible.
- Consistency with the existing warn-and-skip pattern used throughout `CommonFilters`.
- Avoiding silent production of invalid `Ecto.Query` structs that raise at execution time with confusing error messages.

## Considered Options

- **Early validation in the `:bind` reduce** - check binding existence at the entry point where `:at` and `:as` selectors are first processed.
- **Centralized validation in `build_query`** - check binding existence in the shared `build_query` function that all code paths converge on.
- **No validation** - leave the current behavior and let Ecto raise at execution time.

## Decision Outcome

Chosen option: "Early validation in the `:bind` reduce", because it catches invalid inputs at the earliest point where both the binding selector and the query are available, produces clear warning messages that name the exact problem, and follows the existing warn-and-skip pattern.

### Consequences

Good, because callers get an actionable warning message immediately when they target a binding that does not exist, instead of a confusing `ArgumentError` or `FunctionClauseError` at execution time.

Good, because the query is returned unchanged (warn-and-skip), so a single invalid binding entry does not crash the entire filter pipeline.

Bad, because callers that previously relied on Ecto's deferred error (at execution time) will now silently skip the invalid binding instead of raising. This is a deliberate trade-off consistent with ADR 0002.

## Validation

Run the targeted tests that exercise each new validation branch:

    mix test test/ecto_shorts/common_filters/binding_and_boolean_test.exs --seed 0 --trace

Verify these three tests pass:

- "logs a warning and returns the query unchanged when the position exceeds the actual binding count"
- "logs a warning and returns the query unchanged when the position is less than 1"
- "logs a warning and returns the query unchanged when the named binding does not exist"

Run the full suite to confirm no regressions:

    mix test --seed 0 --trace

## Pros and Cons of the Options

### Early validation in the `:bind` reduce

Check `CommonQuery.query_binding_count(q)` for `:at` and `CommonQuery.get_query_binding_source(q, bind_alias)` for `:as` inside the `Enum.reduce` in `create_schema_filter` that processes `:bind` entries.

Good, because the check is co-located with the existing `max_binding_positions` guard, keeping all binding validation in one place.

Good, because it produces specific, actionable warning messages for each failure mode (position < 1, position > actual count, named binding missing).

Bad, because it only catches invalid selectors entering through the `:bind` key, not hypothetical future code paths that might construct `{:at, n}` selectors directly.

### Centralized validation in `build_query`

Add a binding existence check in the private `build_query` function before resolving the source and delegating to builder modules.

Good, because it catches all code paths, including future ones.

Bad, because it requires special-casing `{:as, nil}` (the default root binding selector, always valid) and is further from the user-facing entry point, making warning messages less specific.

### No validation

Leave the current behavior unchanged.

Good, because there is zero implementation risk.

Bad, because callers experience confusing `ArgumentError` or `FunctionClauseError` exceptions at query execution time, far from the actual invalid input.

## More Information

Related: ADR 0002 (warning-only query-building failures) establishes the warn-and-skip pattern that this decision follows.

The validation covers three cases added to `lib/ecto_shorts/common_filters.ex` in the `create_schema_filter` clause that handles `{@binding_selector_key, bind_params}`:

- `:at` with `bind_index < 1`
- `:at` with `bind_index > CommonQuery.query_binding_count(q)`
- `:as` with a named binding that `CommonQuery.get_query_binding_source(q, bind_alias)` returns `nil` for

Revisit this decision if a centralized validation point becomes necessary (for example, if new code paths bypass the `:bind` reduce and pass invalid binding selectors to `build_query` directly).
