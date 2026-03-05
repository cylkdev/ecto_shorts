# Add `:ne` as an alias for `:!=` in comparison filters

---
Status: accepted
Date: 2026-03-01
Deciders: ["EctoShorts maintainers"]
Consulted: []
Informed: ["Library users writing CommonFilters comparison maps"]
---

## Context and Problem Statement

Users can write readable word aliases for most comparison operators, like `:eq`, `:gt`, `:gte`, `:lt`, and `:lte`. Users could not reliably use `:ne` as the matching readable alias for `:!=` across all filter paths. This made the public filter language inconsistent and forced users to switch between symbolic and word forms for inequality. We need one clear rule: if a readable alias exists for equality and ordered comparisons, readable inequality should work the same way.

## Decision Drivers

* Keep the public filter language consistent and predictable.
* Follow existing alias implementation patterns in scalar and array expression builders.
* Avoid breaking existing `:!=` behavior.
* Keep runtime behavior unchanged except for new accepted input shape.
* Keep validation simple through existing test suites.

## Considered Options

* Add `:ne` as a strict alias for `:!=` everywhere alias comparisons are supported.
* Keep only `:!=` for inequality and document that `:ne` is unsupported.
* Introduce a centralized normalization layer that rewrites all operators before dispatch.

## Decision Outcome

Chosen option: "Add `:ne` as a strict alias for `:!=` everywhere alias comparisons are supported", because it directly satisfies consistency and compatibility goals while matching existing per-module alias mapping patterns.

### Consequences

Good, because users can write `%{field: %{ne: value}}` anywhere they already use other word aliases.

Good, because existing `:!=` behavior remains unchanged, so current callers keep working.

Bad, because alias mapping logic remains duplicated across scalar and array spec modules.

Bad, because future alias additions still require touching multiple mapping sites.

## Validation

Run the full suite and confirm zero failures:

    mix test --seed 0 --trace

Confirm coverage includes new `:ne` cases in:

* `test/ecto_shorts/common_filters/scalar_filter_test.exs`
* `test/ecto_shorts/common_filters/array_filter_test.exs`
* `test/ecto_shorts/common_filters/helper_expr_test.exs`
* `test/ecto_shorts/dynamics_test.exs`
* `test/ecto_shorts/compiler/expr_builder_group_specs_test.exs`

## Pros and Cons of the Options

### Add `:ne` alias everywhere alias comparisons are supported

This extends current alias guards and alias-to-canonical mappings to include `:ne -> :!=` in scalar and array builders.

Good, because it preserves current architecture and coding patterns.

Bad, because alias mapping logic is still spread across modules.

Neutral (w.r.t. performance), because alias mapping is simple pattern matching already used for other aliases.

### Keep only `:!=` and reject `:ne`

This keeps current behavior and avoids code changes.

Good, because no implementation effort is required.

Bad, because inequality remains the only comparison without a readable alias pair.

Neutral (w.r.t. internals), because current internals already support alias mapping patterns.

### Build centralized operator normalization

This adds a new preprocessing step to normalize aliases before expression dispatch.

Good, because future aliases might be added in one place.

Bad, because it introduces architectural churn for a small interface gap.

Neutral (w.r.t. user API), because callers still pass the same filter shapes.

## More Information

Related ADRs:

* `docs/adr/0002-warning-only-query-building-failures.md`
* `docs/adr/0003-correct-test-input-assumptions.md`

Key implementation files:

* `lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs.ex`
* `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs.ex`

Revisit this decision if alias logic causes repeated defects or maintenance friction, which would justify centralizing normalization.
