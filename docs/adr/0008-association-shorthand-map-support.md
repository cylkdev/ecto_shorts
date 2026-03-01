# Fix documentation and harden association shorthand to accept maps

---
Status: accepted
Date: 2026-03-01
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

The `CommonFilters` module documentation stated that association shorthand only accepts keyword lists and that passing a map would log a warning and skip the filter. Investigation revealed this documentation was incorrect. The `normalize_filter_params/1` function already converts inner map values to keyword lists before they reach `build_join_filters/7`, so maps worked in practice. However, `build_join_filters/7` itself contained a `Keyword.keyword?/1` guard that would reject maps if they ever bypassed normalization.

Should we remove the incorrect documentation warning and harden `build_join_filters/7` to accept maps directly?

## Decision Drivers

- API consistency: every other filter entry point accepts both maps and keyword lists interchangeably.
- Defense-in-depth: if normalization changes in the future, `build_join_filters/7` should still handle maps gracefully.
- Documentation accuracy: the warning misled users into thinking maps were unsupported.

## Considered Options

1. Fix documentation and harden `build_join_filters/7` with `Map.to_list/1` conversion.
2. Fix documentation only, relying on normalization to handle maps.

## Decision Outcome

Chosen option: "Fix documentation and harden `build_join_filters/7`", because it ensures maps are accepted at every layer, not just through normalization. This makes the code resilient to future refactors of the normalization pipeline.

### Consequences

Good, because maps and keyword lists are now explicitly interchangeable for association shorthand at every layer.
Good, because the documentation no longer misleads users.
Bad, because the `Map.to_list/1` call in `build_join_filters/7` is redundant with normalization today, adding a minor amount of dead code.

## Validation

Run the association shorthand map tests:

    mix test test/ecto_shorts/common_filters/join_test.exs --seed 0 --trace

Verify these tests pass:

- `association map - %{author: %{as: :author, first_name: "John"}}`
- `association map - %{author: %{first_name: "John"}} (no :as)`
- `association map - %{author: %{as: :author, type: :left, first_name: "John"}}`

Verify the invalid-payload test still passes with the updated warning message mentioning "map or keyword list".

## Pros and Cons of the Options

### Fix documentation and harden build_join_filters

Add `Map.to_list/1` conversion at the top of `build_join_filters/7` so it handles maps directly, independent of upstream normalization.

Good, because it makes the function self-contained and resilient to upstream changes.
Good, because it aligns the warning message with the actual contract ("map or keyword list").
Bad, because the conversion is redundant with normalization for the current call path.

### Fix documentation only

Remove the incorrect warning but leave `build_join_filters/7` unchanged.

Good, because it requires no production code change.
Bad, because `build_join_filters/7` still silently depends on normalization converting maps before they arrive.

## More Information

The `normalize_filter_params/1` function at line 1506 of `lib/ecto_shorts/common_filters.ex` converts maps to keyword lists recursively. This is the reason maps already worked before this change.

Revisit this decision if the normalization pipeline is refactored to stop converting nested map values to keyword lists.
