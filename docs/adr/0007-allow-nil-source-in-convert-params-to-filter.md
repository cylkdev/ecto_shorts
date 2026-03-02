# Allow nil source in convert_params_to_filter/3

---
Status: superseded by 0012
Date: 2026-03-01
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

`EctoShorts.CommonFilters.convert_params_to_filter/3` requires a non-nil first argument (a schema module, table name string, `{source, schema}` tuple, or `Ecto.Query`). When the source is determined entirely by data at runtime (for example from an HTTP request payload), the caller must extract the source from the data before calling the function. This forces every caller to handle source extraction, even though the function already supports a `:source` meta-key in the params that can override the first argument.

The question is: should `nil` be a valid first argument, with the `:source` key in params providing the actual source?

## Decision Drivers

* Data-driven API consistency: the params already support a `:source` meta-key, so the first argument should be optional when `:source` is present.
* Caller ergonomics: callers that receive fully data-driven payloads should not need to pre-process the source before calling the function.
* Safety: passing `nil` without `:source` must fail fast with a clear error.

## Considered Options

* Allow nil source with `:source` in params (chosen).
* Keep nil invalid and improve the error message.
* Accept nil in `CommonSchema.normalize_source/1` globally.

## Decision Outcome

Chosen option: "Allow nil source with `:source` in params", because it keeps the data-driven API consistent and avoids forcing callers to extract the source before calling the function.

### Consequences

Good, because callers with fully data-driven payloads can pass `nil` and let the params drive the source.
Good, because the change is backward-compatible - existing non-nil callers are unaffected.
Bad, because `nil` as the first argument only works with map or keyword list params (not non-keyword lists), adding a conditional rule callers must know.

## Validation

Run the targeted tests:

    mix test test/ecto_shorts/common_filters/core_test.exs --seed 0 --trace

Look for the describe block "convert_params_to_filter/3 with nil source". All five tests must pass:

* nil source with `:source` in map params
* nil source with `:source` in keyword list params
* nil source auto-adds `select: true` when `:select` is omitted
* nil source raises when `:source` is missing from params
* nil source raises for non-keyword list params

Run the full suite to confirm no regressions:

    mix test --seed 0 --trace

## Pros and Cons of the Options

### Allow nil source with :source in params

Move `CommonSchema.to_query(source)` from the top of the function into each branch, called after `:source` is popped. Use `source || schema_source` so non-nil callers are unchanged.

Good, because it is a minimal change scoped to one function.
Good, because it follows the existing `:source` meta-key pattern.
Bad, because non-keyword list params with nil source raise, which is a conditional rule.

### Keep nil invalid and improve the error message

Leave the current behavior but change the `ArgumentError` from `normalize_source/1` to mention the `:source` meta-key as an alternative.

Good, because no runtime behavior changes.
Bad, because callers still need to extract the source before calling the function.

### Accept nil in CommonSchema.normalize_source/1 globally

Add a `normalize_source(nil)` clause that returns `{nil, nil}`.

Good, because nil would work everywhere normalize_source is called.
Bad, because `{nil, nil}` has no meaningful table or schema and would break `to_query/1` and other downstream functions that expect at least one non-nil element.

## More Information

The `@spec` for `convert_params_to_filter/3` was updated to include `nil` in the source type union. The `@doc` and the "## Schemaless Queries" moduledoc section were rewritten to document all three source forms and fix an inaccurate claim that `convert_params_to_filter` raises when `:select` is missing (it only raises at repo execution time).

Revisit this decision if `nil` source support is needed for non-keyword list params or if `EctoShorts.Actions` functions need to accept `nil` sources.
