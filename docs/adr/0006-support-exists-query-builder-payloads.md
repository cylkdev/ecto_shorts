# Support map and keyword query-builder payloads for `:exists`

---
Status: accepted
Date: 2026-03-01
Deciders: ["EctoShorts maintainers"]
Consulted: []
Informed: ["Library users building `:exists` filters from plain data"]
---

## Context and Problem Statement

The `:exists` filter accepted pre-built subquery expressions, but users could not provide the same map or keyword payload shape that is already supported by subquery-related helpers like `:all` and `:any`. This made `:exists` harder to compose from request data and forced callers to pre-build queries in a separate step. We need `:exists` to follow the same data-driven query-builder pattern so callers can stay in one payload format.

## Decision Drivers

* Keep the public filtering API consistent across subquery-oriented operators.
* Reuse existing query-builder payload patterns instead of adding a new custom format.
* Preserve warning-and-skip behavior for invalid input.
* Avoid breaking existing `:exists` behavior with pre-built subquery expressions.
* Keep implementation localized with minimal architectural churn.

## Considered Options

* Add map and keyword payload support for `:exists` using the existing helper-expression pipeline.
* Keep `:exists` limited to pre-built subquery expressions only.
* Add a dedicated `:exists_query` filter key for map and keyword payloads.

## Decision Outcome

Chosen option: "Add map and keyword payload support for `:exists` using the existing helper-expression pipeline", because it keeps behavior consistent with existing subquery payload features and preserves backwards compatibility.

### Consequences

Good, because callers can write `%{where: %{exists: %{source: ..., query: ...}}}` and `%{where: %{exists: [source: ..., query: ...]}}` directly.

Good, because `:exists` now supports both `CommonFilters.convert_params_to_filter/3` and `Dynamics.convert_to_dynamic/4` with consistent payload behavior.

Good, because when payloads omit `:select`, `select: true` is applied for `:exists`, which keeps generated `EXISTS` subqueries valid without requiring a pre-build step.

Bad, because operator-path preprocessing now runs helper-expression transformation for all adapter operators, which broadens the code path used for custom operators.

Bad, because `:exists` payload handling is now partly governed by both `CommonFilters` schema-filter routing and `Dynamics` helper preprocessing.

## Validation

Run targeted coverage first:

    mix test test/ecto_shorts/common_filters/join_test.exs test/ecto_shorts/dynamics_test.exs --seed 0 --trace

Run the full suite:

    mix test --seed 0 --trace

Review expected coverage in:

* `test/ecto_shorts/common_filters/join_test.exs`
* `test/ecto_shorts/dynamics_test.exs`

Verify implementation in:

* `lib/ecto_shorts/dynamics.ex`
* `lib/ecto_shorts/common_filters.ex`

## Pros and Cons of the Options

### Add map and keyword payload support for `:exists` using existing helper-expression pipeline

This extends the current helper-expression conversion pattern so `:exists` can consume `:source` and `:query` payloads directly.

Good, because it matches existing data-driven subquery features.

Bad, because it introduces additional branching around `:exists` payload routing.

Neutral (w.r.t. runtime performance), because helper preprocessing already exists and is reused.

### Keep `:exists` limited to pre-built subquery expressions only

This preserves current behavior and requires no code changes.

Good, because there is no implementation risk.

Bad, because callers still need a separate pre-build query step.

Neutral (w.r.t. internals), because existing paths remain unchanged.

### Add a dedicated `:exists_query` filter key for payload-based usage

This would keep current `:exists` semantics untouched while introducing a new key for payload composition.

Good, because old and new behavior are clearly separated.

Bad, because it adds API surface area and duplicates semantics that already fit under `:exists`.

Neutral (w.r.t. migration), because existing calls keep working.

## More Information

Related ADRs:

* `docs/adr/0002-warning-only-query-building-failures.md`
* `docs/adr/0005-add-ne-alias-for-comparison-operators.md`

Revisit this decision if helper preprocessing for operator keys causes repeated defects in unrelated custom operators, which would justify isolating `:exists` payload conversion into a dedicated path.
