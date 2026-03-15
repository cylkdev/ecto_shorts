# Route Normalized Invalid `with_cte` Names to the Existing Entry Boundary

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the `with_cte` warning fix. If the task later narrows to one helper or one test case, that reasoning must still be recorded here. This plan must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

Today a caller can reach an unnecessary warning from the `:with_cte` path when an invalid CTE name travels through nested `to_keyword/1` conversion and arrives at `WithCte.reduce_entries/4` as a single-entry list `[{123, [as: cte_query]}]`. The warning says the whole `:with_cte` payload is invalid even though the real disagreement is narrower: the individual CTE name is malformed.

After this change, the `:with_cte` reducer will recognize that one normalized invalid-name artifact and delegate it to `apply_entry/5`, where `normalize_cte_name/1` already owns invalid-name rejection. The top-level warning remains reserved for truly invalid container shapes and for the still-unsupported list-of-maps shape when the key itself is otherwise valid. A caller who passes an invalid CTE name will continue to receive an unchanged query without an extra top-level warning. Focused tests show the preserved happy path, the preserved invalid-top-level warning path, and the repaired invalid-name path.

## In Scope

- Update `lib/ecto_shorts/common_filters/with_cte.ex` so the entry-reduction boundary delegates the normalized invalid-name artifact to `apply_entry/5`
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
- [x] (2026-03-15 18:47 -04:00) Reproduced the user-observed warning with `mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs:349 --seed 0`; observed `Expected :with_cte params to be a map or keyword list, got: [{123, [as: #Ecto.Query<...>]}]`.
- [x] (2026-03-15 18:50 -04:00) Implemented the narrow reducer-boundary fix in `lib/ecto_shorts/common_filters/with_cte.ex` by extracting `reduce_entry/4` and delegating only the normalized invalid-name singleton-list shape to `apply_entry/5`.
- [x] (2026-03-15 18:50 -04:00) Updated focused regression tests in `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` and `test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs` to assert the invalid-name path no longer emits the top-level warning.
- [x] (2026-03-15 18:50 -04:00) Ran `mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs --seed 0`; 28 tests, 0 failures.

## Milestones

### Milestone 1: Reproduce and pin the warning boundary

Before changing runtime code, exercise the focused `with_cte` path so the plan records which input shape triggers the warning and which function emits it. The goal is not broad investigation; it is to anchor the repair to one observed mismatch.

Run the focused command from `/Users/kurthogarth/Documents/GitHub/ecto_shorts` and capture the warning or test output that proves the current boundary is too strict.

Acceptance means the plan can name the exact observed input shape, the exact warning text, and the boundary that emitted it.

### Milestone 2: Relax the reducer boundary without moving ownership

Edit `lib/ecto_shorts/common_filters/with_cte.ex` so `reduce_entries/4` delegates the one normalized invalid-name artifact that belongs downstream at `apply_entry/5`. The reducer owns top-level container classification. It must not claim that `[[{123, [as: cte_query]}]]` proves the whole `:with_cte` payload is invalid, because that artifact exists only because the invalid-name entry has already been recursively converted by `to_keyword/1`.

Acceptance means invalid top-level `:with_cte` values still emit the existing warning and return the unchanged query, unsupported list-of-maps with otherwise valid names still warn, and malformed entry names reach the existing invalid-name path instead of being rejected too early.

### Milestone 3: Prove the repaired and preserved behaviors

Update the focused ExUnit coverage so the repaired path is explicit. The tests must cover one preserved happy path, one preserved invalid-top-level warning path, and one repaired invalid-name path that now avoids the top-level warning. Because the same `WithCte` builder serves both schema-backed and schemaless queries, run the focused test files for both boundaries.

Acceptance means the changed tests pass and the recorded output states exactly which executed cases were covered.

## Surprises & Discoveries

- Observation: The public caller-visible owner is `EctoShorts.CommonFilters.convert_params_to_filter/3`, but the `:with_cte` behavior itself is implemented by the private builder `EctoShorts.CommonFilters.WithCte` via `lib/ecto_shorts/common_filters/api.ex`.
  Evidence: `lib/ecto_shorts/common_filters/api.ex` delegates `build_query(:with_cte, ...)` directly to `WithCte.build_query/6`.
- Observation: Existing tests already distinguish between truly invalid top-level `:with_cte` params, invalid `:as` payloads, invalid `:materialized`/`:operation` values, invalid CTE names, and the unsupported list-of-maps shape with a valid string key.
  Evidence: `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` contains separate cases for `"keeps the query unchanged when a with_cte entry is a map with a string key (unsupported list-of-maps shape)"` and the invalid-name case.
- Observation: The reproduced warning comes from recursive `to_keyword/1` conversion in `lib/ecto_shorts/common_filters.ex`, not from the original caller shape directly.
  Evidence: `%{with_cte: [%{123 => [as: cte_query]}]}` becomes `with_cte: [[{123, [as: cte_query]}]]`, and `mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs:349 --seed 0` emitted `[warning] [EctoShorts.CommonFilters.WithCte] Expected :with_cte params to be a map or keyword list, got: [{123, [as: #Ecto.Query<...>]}]`.

## Decision Log

- Decision: Keep the public behavioral anchor on `EctoShorts.CommonFilters.convert_params_to_filter/3` and treat `WithCte` as the internal owner of the `:with_cte` branch.
  Rationale: Callers reach the feature through `convert_params_to_filter/3`, but the warning and entry-shape classification live in `lib/ecto_shorts/common_filters/with_cte.ex`.
  Date/Author: 2026-03-15 / Cascade
- Decision: Repair the mismatch at the reducer boundary instead of suppressing the warning at a higher layer.
  Rationale: The warning is only correct for truly invalid top-level `:with_cte` containers. If an entry can be reduced to a single `{name, definition}` pair, the downstream entry classifier should decide whether that name or definition is valid.
  Date/Author: 2026-03-15 / Cascade
- Decision: Keep the fix narrower than “support list-of-maps”.
  Rationale: Live tests already establish that `%{with_cte: [%{"published_posts" => [as: cte_query]}]}` is still an unsupported public shape that should warn. The chosen implementation delegates only the normalized singleton-list artifact when its candidate CTE name is not an atom or string, which removes the misleading warning without broadening the public API.
  Date/Author: 2026-03-15 / Cascade

## Outcomes & Retrospective

Implemented a narrow repair in `lib/ecto_shorts/common_filters/with_cte.ex` by introducing `reduce_entry/4` and routing only the normalized invalid-name singleton-list shape to `apply_entry/5`. Added regression assertions in both `test/ecto_shorts/common_filters/common_filters_with_cte_test.exs` and `test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs` so the invalid-name path now proves the absence of the top-level warning. Focused verification passed with `mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs --seed 0`: 28 tests, 0 failures.

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

For the `:with_cte` branch, callers may omit `:with_cte`, in which case no CTE is applied and the rest of the filter processing continues unchanged. When callers provide `:with_cte`, the accepted top-level public container remains the same live surface proven by the current docs and tests: a map or keyword-list entry collection. Each successful entry must eventually provide an atom or string CTE name and an `:as` definition that is either a query, subquery, or nested filter payload.

The return value remains an `Ecto.Query`. When the top-level `:with_cte` payload is not a supported container, the function logs `"Expected :with_cte params to be a map or keyword list"` and returns the unchanged query. When an individual CTE name is malformed, the query is left unchanged for that entry; this fix preserves that narrower failure behavior instead of converting it into a broader top-level warning.

## Internal Boundary Contracts

The critical private boundary is `EctoShorts.CommonFilters.WithCte.reduce_entries/4`.

Upstream caller: `WithCte.build_query/6` after any top-level map has been converted with `Map.to_list/1`.

Accepted input at this boundary: a list whose members are either direct `{cte_name, cte_definition}` pairs, or the one normalized singleton-list artifact `[{cte_name, cte_definition}]` when `cte_name` is not an atom or binary. The reducer owns iterating in order and recognizing that the latter artifact belongs downstream at invalid-name classification rather than top-level warning.

Produced output: the accumulated `Ecto.Query` after each accepted entry has been delegated to `apply_entry/5`.

Preserved invariants: entry order remains the reduction order; invalid top-level containers still trigger the existing warning and leave the query unchanged; unsupported list-of-maps with otherwise valid names still warn; downstream boundaries keep owning name validation, CTE-definition validation, and option validation.

Forbidden ownership changes: `reduce_entries/4` must not decide whether a CTE name is valid beyond recognizing when a normalized singleton-list artifact should be sent to the existing invalid-name path. It must not absorb `normalize_cte_name/1`, `build_cte_query/4`, `fetch_materialized/2`, or `fetch_operation/2` responsibilities, and it must not turn general list-of-maps input into a newly supported public API shape.

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

`EctoShorts.CommonFilters.convert_params_to_filter(Post, %{with_cte: [%{123 => [as: cte_query]}]}, [])`

`to_keyword/1` recursively rewrites the payload to `with_cte: [[{123, [as: cte_query]}]]`. `WithCte.build_query/6` passes that value to `reduce_entries/4`. The repaired `reduce_entry/4` recognizes the singleton-list artifact with a non-atom, non-binary name and delegates `{123, [as: cte_query]}` to `apply_entry/5`. `normalize_cte_name/1` rejects `123`, and `apply_entry/5` returns the unchanged query without the extra top-level warning.

## Example Mappings

### Story: `with_cte` warning ownership

A caller passes `:with_cte` params through `EctoShorts.CommonFilters.convert_params_to_filter/3`. The system should warn only when the top-level `:with_cte` payload is truly unsupported at the container boundary. If recursive normalization produces the singleton-list artifact for an invalid CTE name, the query should remain unchanged for that entry without the broader top-level warning.

#### Rules:

- When `:with_cte` is omitted, the query is processed without any CTE change from this branch.
- When `:with_cte` is a supported public container of CTE entries, each entry is delegated to `apply_entry/5` in order.
- When `:with_cte` is not a reducible container, the function logs `"Expected :with_cte params to be a map or keyword list"` and returns the unchanged query.
- When recursive normalization produces `[{cte_name, cte_definition}]` for an invalid non-atom, non-binary `cte_name`, the query remains unchanged for that entry and the top-level warning is not emitted for that case.

#### Examples:

    CommonFilters.convert_params_to_filter(Post, %{published: true}, [])
    # => query filtered by published; no `with_cte` behavior applied

    CommonFilters.convert_params_to_filter(Post, %{with_cte: %{published_posts: %{as: cte_query}}}, [])
    # => query with CTE named "published_posts"

    CommonFilters.convert_params_to_filter(Post, %{with_cte: "invalid"}, [])
    # => query unchanged
    # => Logger warning contains "Expected :with_cte params to be a map or keyword list"

    CommonFilters.convert_params_to_filter(Post, %{with_cte: [%{123 => [as: cte_query]}]}, [])
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

Scenario: Invalid CTE name inside the normalized singleton-list artifact
  Given a caller provides `:with_cte` with `%{with_cte: [%{123 => [as: cte_query]}]}`
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
2. Edit `lib/ecto_shorts/common_filters/with_cte.ex` so `reduce_entries/4` delegates the normalized invalid-name singleton-list artifact to `apply_entry/5`.
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

Observed reproduction warning before the fix:

    mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs:349 --seed 0
    18:47:31.568 [warning] [EctoShorts.CommonFilters.WithCte] Expected :with_cte params to be a map or keyword list, got: [{123, [as: #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.published == ^true>]}]

Expected warning text for the preserved invalid-top-level case:

    [warning] [EctoShorts.CommonFilters.WithCte] Expected :with_cte params to be a map or keyword list, got: "invalid"

Target boundary path for the fix:

    EctoShorts.CommonFilters.convert_params_to_filter/3
      -> lib/ecto_shorts/common_filters/api.ex build_query(:with_cte, ...)
      -> EctoShorts.CommonFilters.WithCte.build_query/6
      -> reduce_entries/4
      -> apply_entry/5

Focused verification result after the fix:

    mix test test/ecto_shorts/common_filters/common_filters_with_cte_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_with_cte_test.exs --seed 0
    Compiling 1 file (.ex)
    ............................
    Finished in 0.1 seconds (0.1s async, 0.00s sync)
    28 tests, 0 failures

## Interfaces and Dependencies

Use the existing Elixir, Ecto, and ExUnit stack already present in the repository. The runtime interface being preserved is `EctoShorts.CommonFilters.convert_params_to_filter/3`. The internal functions expected to remain after this change are `EctoShorts.CommonFilters.WithCte.build_query/6`, `reduce_entries/4`, `apply_entry/5`, `normalize_cte_name/1`, `build_cte_query/4`, `fetch_materialized/2`, and `fetch_operation/2`.

Plan revision note: Created this governing ExecPlan on 2026-03-15 to repair an overly broad `with_cte` warning boundary while preserving the existing public behavior and focused test surface. Revised the same day after implementation to record the reproduced warning, the chosen narrow singleton-list delegation fix, and the passing focused verification run.
