# Auto-add select: true for all schemaless sources

---
Status: accepted
Date: 2026-03-02
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

Ecto requires an explicit `:select` clause to execute a query on a bare table string because there is no schema to infer which columns to return. Previously, `EctoShorts.CommonFilters.convert_params_to_filter/3` only auto-added `select: true` when the schemaless source was resolved through the `:from` key. When a bare table string (or `{table, nil}` tuple) was passed directly as the first argument, the caller had to remember to include `:select` manually or Ecto would raise at execution time. This inconsistency meant two equivalent ways of expressing the same query had different requirements.

## Decision Drivers

- Consistent behavior: sources without a schema should behave the same regardless of how they enter the system.
- Reduced surprise: callers should not need to remember an extra step for one code path but not another.
- Backward compatibility: callers who already pass an explicit `:select` must not be affected.

## Considered Options

- Auto-add `select: true` for all schemaless sources.
- Keep the existing behavior and document the difference.

## Decision Outcome

Chosen option: "Auto-add `select: true` for all schemaless sources", because it eliminates the inconsistency between the direct-source and `:from` code paths and removes a common source of runtime errors for callers using bare table strings.

### Consequences

Good, because callers no longer need to remember to include `:select` when passing a bare table string directly. The behavior is now equivalent to the `:from` path.

Good, because existing callers who already pass an explicit `:select` are unaffected - the system checks for an existing `:select` before adding the default.

Bad, because callers who intentionally built a schemaless query without `:select` (for example to inspect the query struct before execution) will now see a `select: true` on the query. This is unlikely in practice since such a query would raise on execution anyway.

## Validation

Run the targeted tests:

    mix test test/ecto_shorts/common_filters/core_test.exs --seed 0 --trace

The following tests confirm the behavior:

- "adds default select when the source is a bare table string and select is not given"
- "adds default select when the source is a {table, nil} tuple and select is not given"
- "preserves explicit select when the source is a bare table string"
- "adds default select when resolved source is schemaless and select is not given" (existing :from path test)

Run the full suite to confirm no regressions:

    mix test --seed 0 --trace

## Pros and Cons of the Options

### Auto-add select: true for all schemaless sources

Remove the `has_from?` guard in `convert_params_to_filter/3` so the default-select logic triggers whenever the normalized source is `{table, nil}`, regardless of how the source was provided.

Good, because both code paths behave identically.
Good, because the change is a single-line condition removal.
Bad, because it is a subtle behavior change for direct bare-string callers who did not previously get a default select.

### Keep the existing behavior and document the difference

Leave the code as-is and add documentation explaining that direct table-string callers must include `:select`.

Good, because no code change is needed.
Bad, because the inconsistency remains and callers must remember the difference.
Bad, because runtime errors from missing `:select` will continue for the direct path.

## More Information

The moduledoc in `EctoShorts.CommonFilters` (section "`:select` and schemaless queries") has been updated to reflect the new unified behavior. The README caveat about requiring explicit `:select` for schemaless queries has been removed.

Revisit this decision if Ecto adds native support for default select on bare table sources.
