# Decouple Dynamics from subquery payload resolution

---
Status: accepted
Date: 2026-03-03
Deciders: ["EctoShorts maintainers"]
Consulted: []
Informed: ["Library users calling `CommonFilters` and `Dynamics` directly"]
---

## Context and Problem Statement

`EctoShorts.Dynamics` included `prewalk_subqueries` logic that recursively
traversed values and built queries from `%{from: ...}` payloads. That crossed
the intended boundary: Dynamics should compose and delegate dynamic expressions,
not build queries.

The same `%{from: ...}` conversion logic also appeared in multiple places
(`Filter`, `Join`, `WithCte`), which created duplication and made behavior
harder to reason about.

## Decision Drivers

* Keep `Dynamics` as a composition-only dispatcher.
* Avoid recursive prewalk/normalization passes for subquery payload handling.
* Resolve query-builder payloads as operation-scoped units (`:exists`, `:all`, `:any`).
* Reuse one shared conversion core for all `%{from: ...}` payload-to-query work.

## Considered Options

* Keep subquery prewalk in `Dynamics`.
* Move all payload resolution into adapters.
* Add a CommonFilters-side shared resolver and make `Dynamics` strict.

## Decision Outcome

Chosen option: "Add a CommonFilters-side shared resolver and make `Dynamics`
strict."

### Consequences

Good, because `Dynamics` is now dumb by design: boolean grouping, adapter
dispatch, and dynamic composition only.

Good, because `%{from: ...}` query building is centralized in one reusable core
function (`payload_to_query/4`) used by `Filter`, `Join`, and `WithCte`.

Good, because subquery conversion is operation-scoped (`resolve_exists/3`,
`resolve_field_value/4`) instead of a recursive whole-structure prewalk.

Bad, because direct `Dynamics.convert_to_dynamic/4` callers no longer get
implicit `%{from: ...}` payload-to-query conversion.

## Validation

Run targeted tests:

    mix test test/ecto_shorts/common_filters/subquery_payload_test.exs test/ecto_shorts/query_builder/dynamics_test.exs --seed 0 --trace

Run full suite:

    mix test --seed 0 --trace

## More Information

Implementation modules:

* `lib/ecto_shorts/common_filters/subquery_payload.ex`
* `lib/ecto_shorts/common_filters/filter.ex`
* `lib/ecto_shorts/common_filters/join.ex`
* `lib/ecto_shorts/common_filters/with_cte.ex`
* `lib/ecto_shorts/dynamics.ex`
