# Relax `with_cte` Entry Acceptance Without Broadening the Public API

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the `with_cte` warning fix. If the task later narrows to one helper or one test case, that reasoning must still be recorded here. This plan must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

Today a caller can reach an unnecessary warning from the `:with_cte` path even when the input has already been reduced to a CTE entry container that should simply flow to the entry-level validation boundary. The warning says the whole `:with_cte` payload is invalid even though the real disagreement is narrower: an individual CTE entry or CTE name is malformed.

After this change, the `:with_cte` reducer will accept the same entry containers that its downstream `apply_entry/5` boundary is already designed to classify, and it will reserve the top-level warning for truly invalid container shapes. A caller who passes an invalid CTE name will continue to receive an unchanged query without an extra top-level warning. Focused tests will show the preserved happy path, the preserved invalid-top-level warning path, and the repaired invalid-name path.

## In Scope

- Update `lib/ecto_shorts/common_filters/with_cte.ex` so the entry-reduction boundary accepts the narrow entry shapes that should be delegated to `apply_entry/5`
- Preserve the current `apply_entry/5` failure contract for invalid CTE names: return the query unchanged without introducing a new warning there
- Add or revise focused regression tests in `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` for the repaired warning boundary
- Run focused `with_cte` test coverage that exercises both schema-backed and schemaless paths through the shared builder module

## Out of Scope

- Adding new public `with_cte` payload families beyond the shapes already implied by the live code and tests
- Changing `:materialized`, `:operation`, or `:as` behavior beyond what is required for this warning fix
- Changing `CommonFilters.convert_params_to_filter/3` ordering, normalization, or unrelated filter dispatch behavior
- Documentation-only cleanup outside this governing plan

## Progress

- [x] (2026-03-15 18:31 -04:00) Read `.agent/RULES.md`, `.agent/PLANS.md`, and the module/function/testing guides required for this task.
- [x] (2026-03-15 18:34 -04:00) Traced the live boundary path: `EctoShorts.CommonFilters.convert_params_to_filter/3` -> `lib/ecto_shorts/common_filters/api.ex` `build_query/6` -> `EctoShorts.CommonFilters.WithCte.build_query/6` -> `reduce_entries/4` -> `apply_entry/5`.
- [x] (2026-03-15 18:37 -04:00) Inventoried the existing `with_cte` tests in `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` and `test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs`, including the invalid-name and invalid-top-level warning cases.
- [ ] Reproduce the user-observed warning with a focused command so the repaired boundary is grounded in execution evidence.
- [ ] Implement the narrow reducer-boundary fix in `lib/ecto_shorts/common_filters/with_cte.ex`.
- [ ] Add or update regression tests for the repaired warning behavior.
- [ ] Run the focused `with_cte` test files and record the exact results.

## Milestones

### Milestone 1: Reproduce and pin the warning boundary

Before changing runtime code, exercise the focused `with_cte` path so the plan records which input shape triggers the warning and which function emits it. The goal is not broad investigation; it is to anchor the repair to one observed mismatch.

Run the focused command from `/Users/kurthogarth/Documents/GitHub/ecto_shorts` and capture the warning or test output that proves the current boundary is too strict.

Acceptance means the plan can name the exact observed input shape, the exact warning text, and the boundary that emitted it.

### Milestone 2: Relax the reducer boundary without moving ownership

Edit `lib/ecto_shorts/common_filters/with_cte.ex` so `reduce_entries/4` accepts the entry containers that belong downstream at `apply_entry/5`. The reducer owns only top-level container classification. It must not reject an entry shape that can be converted into one `{cte_name, cte_definition}` pair and then classified by `normalize_cte_name/1`, `build_cte_query/4`, `fetch_materialized/2`, and `fetch_operation/2`.

Acceptance means invalid top-level `:with_cte` values still emit the existing warning and return the unchanged query, while malformed entry names reach the existing invalid-name path instead of being rejected too early.

### Milestone 3: Prove the repaired and preserved behaviors

Update the focused ExUnit coverage so the repaired path is explicit. The tests must cover one preserved happy path, one preserved invalid-top-level warning path, and one repaired invalid-name path that now avoids the top-level warning. Because the same `WithCte` builder serves both schema-backed and schemaless queries, run the focused test files for both boundaries.

Acceptance means the changed tests pass and the recorded output states exactly which executed cases were covered.

## Surprises & Discoveries

- Observation: The public caller-visible owner is `EctoShorts.CommonFilters.convert_params_to_filter/3`, but the `:with_cte` behavior itself is implemented by the private builder `EctoShorts.CommonFilters.WithCte` via `lib/ecto_shorts/common_filters/api.ex`.
  Evidence: `lib/ecto_shorts/common_filters/api.ex` delegates `build_query(:with_cte, ...)` directly to `WithCte.build_query/6`.
- Observation: Existing tests already distinguish between truly invalid top-level `:with_cte` params, invalid `:as` payloads, invalid `:materialized`/`:operation` values, and invalid CTE names.
  Evidence: `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` contains separate tests for each case, including `"keeps the query unchanged when the with_cte name is invalid"`.

## Decision Log

- Decision: Keep the public behavioral anchor on `EctoShorts.CommonFilters.convert_params_to_filter/3` and treat `WithCte` as the internal owner of the `:with_cte` branch.
  Rationale: Callers reach the feature through `convert_params_to_filter/3`, but the warning and entry-shape classification live in `lib/ecto_shorts/common_filters/with_cte.ex`.
  Date/Author: 2026-03-15 / Cascade
- Decision: Repair the mismatch at the reducer boundary instead of suppressing the warning at a higher layer.
  Rationale: The warning is only correct for truly invalid top-level `:with_cte` containers. If an entry can be reduced to a single `{name, definition}` pair, the downstream entry classifier should decide whether that name or definition is valid.
  Date/Author: 2026-03-15 / Cascade

## Outcomes & Retrospective

Pending implementation and focused verification.

## Context and Orientation

`EctoShorts.CommonFilters` is the public query-filter entry point. Callers use `EctoShorts.CommonFilters.convert_params_to_filter/3` to turn a map or keyword list of filter params into an `Ecto.Query`. The public docs in `lib/ecto_shorts/common_filters.ex` describe `:recursive_ctes` and `:with_cte` as the common table expression surface.

`lib/ecto_shorts/common_filters/api.ex` is the internal dispatch table. When a filter key is `:with_cte`, `build_query/6` delegates to `EctoShorts.CommonFilters.WithCte.build_query/6`.

`lib/ecto_shorts/common_filters/with_cte.ex` is a private builder module. It classifies the top-level `:with_cte` value, reduces each CTE entry, normalizes the CTE name, builds the CTE query from a prebuilt query, subquery, or nested filter params, and then calls `Ecto.Query.with_cte/3`.

The focused tests live in two files. `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` covers schema-backed sources such as `EctoShorts.Schema.Post`. `test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs` covers schemaless sources such as `"posts"`. Both call the same public function and flow through the same `WithCte` builder.

## Module Specification

The public module boundary relevant to this task is `EctoShorts.CommonFilters`. Use this module when a caller has filter params and needs an `Ecto.Query` built from those params. Do not use it to validate one isolated `with_cte` entry in isolation; the module owns the whole filter-to-query conversion flow.

The entry point that matters here is `convert_params_to_filter/3`. Its module-level rule for this task is that recognized filter keys delegate to their owning builder modules, and invalid inputs are generally handled by logging a warning and leaving the query unchanged rather than raising. `EctoShorts.CommonFilters.Api` participates as an internal dispatcher only. `EctoShorts.CommonFilters.WithCte` participates as the internal owner of the `:with_cte` branch, not as a public entry point.

## Function Specification

The public function boundary relevant to this task is `EctoShorts.CommonFilters.convert_params_to_filter/3`.

For the `:with_cte` branch, callers may omit `:with_cte`, in which case no CTE is applied and the rest of the filter processing continues unchanged. When callers provide `:with_cte`, the accepted top-level container is a map or list-shaped collection of CTE entries that the `WithCte` branch can reduce into individual `{cte_name, cte_definition}` pairs. Each successful entry must eventually provide an atom or string CTE name and an `:as` definition that is either a query, subquery, or nested filter payload.

The return value remains an `Ecto.Query`. When the top-level `:with_cte` payload is not a reducible container, the function logs `"Expected :with_cte params to be a map or keyword list"` and returns the unchanged query. When an individual CTE name is malformed, the query is left unchanged for that entry; this plan preserves that narrower failure behavior instead of converting it into a broader top-level warning.

## Internal Boundary Contracts

The critical private boundary is `EctoShorts.CommonFilters.WithCte.reduce_entries/4`.

Upstream caller: `WithCte.build_query/6` after any top-level map has been converted with `Map.to_list/1`.

Accepted input at this boundary: a list whose members are either direct `{cte_name, cte_definition}` pairs or single-entry map containers that can be converted into exactly one such pair. The reducer owns converting those containers into entry pairs and iterating in order.

Produced output: the accumulated `Ecto.Query` after each accepted entry has been delegated to `apply_entry/5`.

Preserved invariants: entry order remains the reduction order; invalid top-level containers still trigger the existing warning and leave the query unchanged; downstream boundaries keep owning name validation, CTE-definition validation, and option validation.

Forbidden ownership changes: `reduce_entries/4` must not decide whether a CTE name is valid beyond being able to extract one candidate key/value pair. It must not absorb `normalize_cte_name/1`, `build_cte_query/4`, `fetch_materialized/2`, or `fetch_operation/2` responsibilities.

The next private boundary is `EctoShorts.CommonFilters.WithCte.apply_entry/5`.

Upstream caller: `reduce_entries/4`.

Accepted input at this boundary: a query accumulator, a candidate `cte_name`, a candidate `cte_definition`, and `opts`.

Produced output: either a query with one additional CTE applied, or the unchanged query when `normalize_cte_name/1` or later validation fails.

Preserved invariants: invalid CTE names continue to fail here, not earlier; `:as`, `:materialized`, and `:operation` validation continue to emit their existing warnings at their current boundaries.

## Internal Structure Walkthrough

Happy path example:

`EctoShorts.CommonFilters.convert_params_to_filter(Post, %{with_cte: %{published_posts: %{as: cte_query}}}, [])`

`convert_params_to_filter/3` eventually dispatches `:with_cte` through `lib/ecto_shorts/common_filters/api.ex` to `WithCte.build_query/6`. `WithCte.build_query/6` converts the top-level map to `[{:published_posts, %{as: cte_query}}]`, then calls `reduce_entries/4`. `reduce_entries/4` passes the pair to `apply_entry/5`. `apply_entry/5` normalizes `:published_posts` to `"published_posts"`, validates the `:as` query, and applies the CTE.

Invalid-top-level example:

`EctoShorts.CommonFilters.convert_params_to_filter(Post, %{with_cte: "invalid"}, [])`

`WithCte.build_query/6` sends `"invalid"` to `reduce_entries/4`. The non-list clause logs `"Expected :with_cte params to be a map or keyword list"` and returns the original query.

Repaired invalid-name example:

A reducible entry container reaches `reduce_entries/4`, is converted to one `{cte_name, cte_definition}` pair, and is then delegated to `apply_entry/5`. If `normalize_cte_name/1` rejects the name, `apply_entry/5` returns the unchanged query for that entry without an extra top-level warning. The repair is that `reduce_entries/4` no longer claims the entire `:with_cte` payload is invalid when the mismatch really belongs to this downstream invalid-name path.

## Example Mappings

### Story: `with_cte` warning ownership

A caller passes `:with_cte` params through `EctoShorts.CommonFilters.convert_params_to_filter/3`. The system should warn only when the top-level `:with_cte` payload is not reducible as a CTE-entry container. If the payload is reducible and the problem is only that one CTE name is malformed, the query should remain unchanged for that entry without the broader top-level warning.

#### Rules:

- When `:with_cte` is omitted, the query is processed without any CTE change from this branch.
- When `:with_cte` is a reducible container of CTE entries, each entry is delegated to `apply_entry/5` in order.
- When `:with_cte` is not a reducible container, the function logs `"Expected :with_cte params to be a map or keyword list"` and returns the unchanged query.
- When one delegated entry has an invalid CTE name, the query remains unchanged for that entry and the top-level warning is not emitted for that case.

#### Examples:

    CommonFilters.convert_params_to_filter(Post, %{published: true}, [])
    # => query filtered by published; no `with_cte` behavior applied

    CommonFilters.convert_params_to_filter(Post, %{with_cte: %{published_posts: %{as: cte_query}}}, [])
    # => query with CTE named "published_posts"

    CommonFilters.convert_params_to_filter(Post, %{with_cte: "invalid"}, [])
    # => query unchanged
    # => Logger warning contains "Expected :with_cte params to be a map or keyword list"

    CommonFilters.convert_params_to_filter(Post, %{with_cte: %{123 => %{as: cte_query}}}, [])
    # => query unchanged for that entry
    # => no top-level `with_cte` container warning for the invalid-name case

#### Open Questions:

- **Q:** Should the fix introduce a brand-new public `with_cte` input family? **A:** No. Keep the repair narrow and preserve the current public surface.
- **Q:** Which boundary should own invalid CTE names? **A:** `apply_entry/5` via `normalize_cte_name/1`, not `reduce_entries/4`.

## Behaviour Specifications

### Feature: `with_cte` warns only for invalid top-level containers

Scenario: Omitted `with_cte`
  Given a caller does not provide `:with_cte`
  When `convert_params_to_filter/3` is called with other valid filter params
  Then the query is built without any `with_cte` warning from this branch

Scenario: Invalid top-level `with_cte` payload
  Given a caller provides `:with_cte` with a non-container value
  When `convert_params_to_filter/3` is called
  Then the returned query is unchanged
  And a warning is emitted containing `Expected :with_cte params to be a map or keyword list`

Scenario: Invalid CTE name inside a reducible entry container
  Given a caller provides `:with_cte` with a reducible entry whose CTE name is not an atom or string
  When `convert_params_to_filter/3` is called
  Then the returned query is unchanged for that entry
  And the top-level `with_cte` warning is not emitted for that case

## Executable Tests

Update `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` to express three claims:

1. A preserved happy path for a valid `with_cte` entry still produces the expected query.
2. An invalid top-level `with_cte` payload still logs the existing warning and leaves the query unchanged.
3. A reducible invalid-name entry leaves the query unchanged without logging the top-level `with_cte` warning.

If the same repaired path is not already covered adequately by the schemaless suite, add the narrow equivalent coverage in `test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs`. Prefer the smallest test set that proves the shared builder behavior from both public source families.

## Concrete Steps

All commands run from `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

1. Reproduce the user-observed warning with a focused command or test invocation and record the emitted text.
2. Edit `lib/ecto_shorts/common_filters/with_cte.ex` so `reduce_entries/4` accepts reducible entry containers and delegates malformed names to `apply_entry/5`.
3. Update the focused `with_cte` tests to prove the repaired warning boundary.
4. Run:

    mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs

5. Run:

    mix test test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs

6. Record the exact executed cases and outcomes in this plan.

## Validation and Acceptance

The validation claim is limited to deterministic `with_cte` behavior exercised by the focused ExUnit cases. Passing tests will show that the executed valid case, invalid-top-level case, and repaired invalid-name case behave as expected for the covered schema-backed and schemaless inputs. They will not prove correctness for all possible `with_cte` payloads.

Acceptance requires:

- The user-observed warning is reproduced or otherwise traced to the exact reducer boundary before the fix.
- The implementation change is limited to `lib/ecto_shorts/common_filters/with_cte.ex` unless a test-only supporting edit is needed.
- The focused `with_cte` tests pass after the change.
- This plan is updated with the actual executed evidence and final outcome.

## Idempotence and Recovery

The implementation edit is limited to one private builder module and focused tests. Re-running the test commands is safe. If the change proves incorrect, revert the edited portions of `lib/ecto_shorts/common_filters/with_cte.ex` and the added regression test assertions, then re-run the same focused tests to confirm the prior state.

## Artifacts and Notes

Expected warning text for the preserved invalid-top-level case:

    [warning] [EctoShorts.CommonFilters.WithCte] Expected :with_cte params to be a map or keyword list, got: "invalid"

Target boundary path for the fix:

    EctoShorts.CommonFilters.convert_params_to_filter/3
      -> lib/ecto_shorts/common_filters/api.ex build_query(:with_cte, ...)
      -> EctoShorts.CommonFilters.WithCte.build_query/6
      -> reduce_entries/4
      -> apply_entry/5

## Interfaces and Dependencies

Use the existing Elixir, Ecto, and ExUnit stack already present in the repository. The runtime interface being preserved is `EctoShorts.CommonFilters.convert_params_to_filter/3`. The internal functions expected to remain after this change are `EctoShorts.CommonFilters.WithCte.build_query/6`, `reduce_entries/4`, `apply_entry/5`, `normalize_cte_name/1`, `build_cte_query/4`, `fetch_materialized/2`, and `fetch_operation/2`.

Plan revision note: Created this governing ExecPlan on 2026-03-15 to repair an overly broad `with_cte` warning boundary while preserving the existing public behavior and focused test surface.
