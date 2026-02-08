# Understanding Summary

## Goal
Identify the root cause of the failing join assertion in `test/ecto_shorts/common_filters_test.exs:985`.

## Constraints
- Diagnose only; do not change runtime behavior.
- Keep analysis scoped to the failing join path.

## Inputs
- Failing test output for `mix test test/ecto_shorts/common_filters_test.exs:985`.
- Relevant code paths:
  - `lib/ecto_shorts/common_filters.ex`
  - `lib/ecto_shorts/common_filters/join.ex`
  - `test/ecto_shorts/common_filters_test.exs`

## Outputs
- A concrete root-cause explanation tied to exact call flow and code locations.

## Acceptance Criteria
- Explain why the join warning appears and why no join is added.
- Confirm whether the issue is isolated or affects related canonical join shapes.

## Unknowns
- DB connectivity logs (`localhost:5432`, `:eperm`) are present in this sandbox but not required to reproduce the SQL mismatch root cause.
