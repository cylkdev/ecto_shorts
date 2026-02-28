# Correct Test Input Assumptions to Map Format

---
Status: accepted
Date: 2026-02-28
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

The test suite for `EctoShorts.CommonFilters.convert_params_to_filter/3` encoded six tests using internal tuple format (`{:key, value}`) for operator expressions, while the public API documented in the input catalog (`convert-params-to-filter-inputs-df7375.md`) specifies map format (`%{key: value}`). This mismatch meant the tests were validating an internal representation rather than the user-facing API shape. Additionally, many input shapes from the catalog had zero test coverage.

## Decision Drivers

The input catalog serves as the single source of truth for what `convert_params_to_filter/3` accepts. Tests should exercise the public API surface, not internal normalization artifacts. Missing coverage for documented input shapes means regressions can slip through undetected.

## Considered Options

Correct the six tuple-format tests to use map format and add coverage for uncovered input shapes from the catalog.

Leave the tuple-format tests as-is and only add new coverage.

## Decision Outcome

Chosen option: "Correct the six tuple-format tests to use map format and add coverage", because the catalog defines the public API contract and tests should validate that contract. The normalization pipeline already converts maps to the same internal tuples, so no production code changes were needed.

### Consequences

Good, because all six corrected tests pass without production code changes, confirming the normalization pipeline handles map format correctly.

Good, because 62 new tests cover previously untested input shapes including custom filters (`:ids`, `:after`, `:before`, `:start_date`, `:end_date`), comparison aliases (`gt`, `gte`, `lt`, `lte`, `eq`), scalar `:any`/`:all` subquery helpers, negated `:all`/`:any`, aggregate operators (`count`, `max`, `min`, `sum`), aggregate aliases, date/time helpers (`date_add`, `from_now`, `ago`), arithmetic operators (`-`, `*`, `/`), negated comparisons, array alias operators, array aggregates, array LOWER/UPPER via explicit `==`/`!=`, `:last` with sort keys, meta-key extraction, explicit nil operators, and double-negation NOT IN.

Bad, because existing code that uses the tuple format directly still works but is not the documented API shape. This could cause confusion if users copy test patterns.

## Validation

Run the full test suite to confirm all tests pass:

    mix test

The common filters test file should report 307 tests, 0 failures (up from the original 245).

Run quality checks:

    mix credo --strict
    mix dialyzer

Both should pass with no new warnings.

## Pros and Cons of the Options

### Correct tuple-format tests and add coverage

Aligns tests with the documented public API. Catches any future normalization regressions. No production code changes required.

Good, because it validates the API users actually call.
Good, because 62 new tests significantly reduce the risk of undetected regressions.
Bad, because it does not remove tuple-format support from the code, so the internal format remains an undocumented alternative.

### Leave tuple-format tests as-is

Avoids changing existing green tests.

Good, because zero risk of breaking existing tests.
Bad, because tests continue to validate an internal representation, not the public API.
Bad, because missing coverage remains.

## More Information

The input catalog is at `convert-params-to-filter-inputs-df7375.md` in the repository root. The implementation plan is at `.windsurf/plans/fix-test-assumptions-and-coverage-98dab7.md`. Revisit this decision if the normalization pipeline is refactored to no longer accept map format, or if a decision is made to deprecate the tuple format entirely.
