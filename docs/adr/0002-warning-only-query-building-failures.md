# Convert CommonFilters query-building failures to warning-and-skip behavior

---
Status: accepted
Date: 2026-02-28
Deciders: ["Project maintainers"]
Consulted: ["Library users through issue feedback"]
Informed: ["Contributors working in `EctoShorts.CommonFilters` and related query builders"]
---

## Context and Problem Statement

`EctoShorts.CommonFilters.convert_params_to_filter/3` is intended to build queries from data-driven filter payloads. Before this decision, some invalid payloads logged warnings and were skipped, while others raised `ArgumentError`, `Ecto.QueryError`, or `Ecto.Query.CompileError` during query construction. This created an inconsistent contract for callers and made dynamic filter ingestion brittle. We need one consistent failure model that preserves query construction progress and avoids runtime interruption from malformed filter entries.

## Decision Drivers

* Keep the public query-building API resilient when filter payloads are partially invalid.
* Make failure behavior consistent across all filter operations, including failures raised by Ecto query-building calls.
* Eliminate silent skips so operators can diagnose bad payloads from warning logs.
* Reduce accidental broad joins from invalid `:join` `:on` payload fallback behavior.

## Considered Options

* Warning-and-skip for all query-building failures (including Ecto-raised errors).
* Keep current mixed behavior (some warnings, some raises).
* Warn-and-skip only for unknown keys, but keep raises for structural and Ecto-level errors.

## Decision Outcome

Chosen option: "Warning-and-skip for all query-building failures (including Ecto-raised errors)", because it directly satisfies resilience and consistency drivers while preserving observability through explicit warning logs.

### Consequences

Good, because callers can submit dynamic filter payloads without having the whole query build fail when one entry is invalid.

Good, because logs now consistently explain what failed and what was skipped, including exception messages from Ecto-level query construction.

Good, because invalid `:join` `:on` payloads no longer default to `on: true`; the join entry is skipped, which avoids unintended broad joins.

Bad, because invalid payloads may be overlooked in environments where warning logs are not monitored.

Bad, because wrapping broad query-building paths can hide programmer mistakes that would otherwise fail fast during development.

## Validation

Validate the decision with these commands from repository root:

    mix test test/ecto_shorts/common_filters_test.exs

The following behaviors must be observed in tests:

* Invalid payload tests no longer use `assert_raise`; they assert warning logs and unchanged query output.
* Invalid `:join` `:on` payload test asserts warning log and no join is applied.
* Invalid `:with_ties` and `:distinct` scenarios are handled as warning-and-skip behavior.

Then run full quality checks:

    mix credo --strict
    mix dialyzer
    mix test

## Pros and Cons of the Options

### Warning-and-skip for all query-building failures (including Ecto-raised errors)

This option wraps query-building operations and converts exceptions into warning logs while returning the unchanged query for the failing operation.

Good, because it gives one predictable contract for callers.

Good, because it preserves partial progress for valid filter entries in mixed payloads.

Neutral (w.r.t. performance), because exception handling is only exercised on invalid paths.

Bad, because it may delay discovery of malformed payload producers if warnings are ignored.

### Keep current mixed behavior (some warnings, some raises)

This option preserves existing behavior where unknown keys are often skipped but some payloads still raise.

Good, because existing fast-fail paths remain explicit in development.

Bad, because callers must guess which invalid payloads will raise and which will not.

Bad, because dynamic ingestion use cases remain brittle.

### Warn-and-skip only for unknown keys, but keep raises for structural and Ecto-level errors

This option keeps warning behavior for unknown fields only and preserves raises for many invalid operation payloads.

Good, because some severe invalid payloads still fail fast.

Bad, because it still leaves inconsistent behavior at the API boundary.

Neutral (w.r.t. migration effort), because it requires fewer code changes than a full warning-only model.

## More Information

Implemented in:

* `lib/ecto_shorts/common_filters.ex`
* `lib/ecto_shorts/common_filters/join.ex`
* `test/ecto_shorts/common_filters_test.exs`

Related artifact:

* `docs/plans/0001-common-filters-warning-no-raise.md`

Revisit this decision if warning volume becomes noisy enough to hide real operational signals, or if maintainers need stricter fail-fast behavior in development-only environments.
