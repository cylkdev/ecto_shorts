# Make CommonFilters warning-only on query-building failures

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan follows `.agent/PLANS.md` and must be maintained in accordance with it.

## Purpose / Big Picture

After this change, callers of `EctoShorts.CommonFilters.convert_params_to_filter/3` should never see query-building failures raise from invalid filter payloads. Instead, the library should log a warning and skip only the failing operation while preserving the rest of the query build. This includes errors raised internally by Ecto query-building calls (for example invalid `:with_ties` or `:distinct` payloads). You can see this working by running the `CommonFilters` tests and observing warning logs plus unchanged query assertions for invalid inputs.

## Progress

**Legend**

[ ] - Not started
[~] - In progress
[x] - Completed

- [x] (2026-02-28 19:03Z) Investigated current `CommonFilters`, `Join`, `BindingParams`, `Filter`, and `Dynamics` behavior and mapped raise/warn/skip paths.
- [x] (2026-02-28 19:04Z) Confirmed product decisions from user: catch Ecto-raised query-building errors; invalid `:join` `:on` payload must skip join entry.
- [x] (2026-02-28 19:07Z) Added failing boundary tests converting raise expectations to warning+skip behavior and added regressions for invalid association params and invalid `:join` `:on` payload.
- [x] (2026-02-28 19:10Z) Implemented warning+skip behavior in `CommonFilters` operation boundaries and exception wrapping.
- [x] (2026-02-28 19:12Z) Implemented `Join` `:on` validation as warning+skip join entry behavior.
- [x] (2026-02-28 19:13Z) Updated `CommonFilters` and `Join` moduledocs to remove silent-skip wording and describe warning+skip semantics.
- [x] (2026-02-28 19:16Z) Completed `/run-checks` + `/code-style-review` loop with `mix credo --strict`, `mix dialyzer`, `mix test`, and formatting checks all passing.
- [x] (2026-02-28 19:17Z) Completed `/write-docs` focused update for changed modules.
- [x] (2026-02-28 19:18Z) Wrote ADR `docs/adr/0002-warning-only-query-building-failures.md`.

## Surprises & Discoveries

- Observation: Some paths already warn+skip correctly (unknown schema fields), but at least one path skips silently (`build_join_filters/7` when assoc params are not keyword).
  Evidence: `lib/ecto_shorts/common_filters.ex` returns `query` directly in that branch without `Logger.warning`.

- Observation: Invalid `:bind` selector entries are handled as operation-level skip, not per-entry continuation.
  Evidence: Mixed invalid/valid `:bind -> :at` input now logs warning and leaves query unchanged, because `BindingParams.build_binding_params/6` raises before returning partial progress.

## Decision Log

- Decision: Query-building should not raise, including Ecto compile/runtime errors raised during query construction.
  Rationale: The user explicitly requested warning+skip behavior for those failures (for example `:with_ties` and `:distinct`).
  Date/Author: 2026-02-28 / Cascade

- Decision: Invalid `:join` `:on` payloads should skip the entire join entry.
  Rationale: The previous fallback (`on: true`) can apply an unintended join; skipping is safer and explicit.
  Date/Author: 2026-02-28 / Cascade

## Outcomes & Retrospective

The implementation now enforces a consistent warning-and-skip failure contract across `convert_params_to_filter/3` query building. Invalid payloads no longer raise at the API boundary; they log a warning and skip the failing operation. The join `:on` fallback behavior was tightened so invalid `:on` payloads skip the join entry rather than defaulting to `on: true`, reducing risk of accidental broad joins.

Behavior was proven through updated boundary tests in `test/ecto_shorts/common_filters_test.exs`, including formerly raise-based scenarios (`:bind`, `:with_ties`, `:distinct`, invalid nil operator, and missing preload bind alias). Validation commands (`mix credo --strict`, `mix dialyzer`, `mix test`) all pass.

## Context and Orientation

`EctoShorts.CommonFilters.convert_params_to_filter/3` (in `lib/ecto_shorts/common_filters.ex`) is the public boundary for converting data-driven filters into `Ecto.Query` values. It delegates per-filter behavior to submodules under `lib/ecto_shorts/common_filters/`, especially `filter.ex`, `join.ex`, and `bind_params.ex`. `test/ecto_shorts/common_filters_test.exs` is the primary behavior-spec test file and currently includes many `assert_raise` expectations that must be converted to warning+skip expectations.

In this repository, "skip" means return the current query unchanged for the failing entry while continuing to process the rest. "Warning" means calling `EctoShorts.Logger.warning/2` with a helpful message that includes the invalid payload and reason where possible.

## Plan of Work

First, update tests in `test/ecto_shorts/common_filters_test.exs` to encode the desired boundary behavior: invalid payloads should not raise and should leave the query unchanged while emitting warnings. Include a new regression test for invalid association join-filter params that currently skip silently.

Second, implement behavior changes primarily in `lib/ecto_shorts/common_filters.ex` and `lib/ecto_shorts/common_filters/join.ex`. Add safe wrapping around submodule query-building calls so exceptions raised by Ecto query builders are converted into warning+skip behavior. Update explicit raise paths in direct `CommonFilters` code to warning+skip behavior. Ensure bind-related failures are also converted from raise to warning+skip at API boundary.

Third, adjust join `:on` handling in `lib/ecto_shorts/common_filters/join.ex` so invalid `:on` payloads return an error sentinel and the join entry is skipped, with warning log.

Fourth, update docs in `lib/ecto_shorts/common_filters.ex` to remove "silently skipped" wording and document warning+skip semantics.

## Concrete Steps

From repo root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`:

1. Run targeted test file to capture baseline failures after new tests are added:
   `mix test test/ecto_shorts/common_filters_test.exs`
2. Implement warning+skip behavior in `CommonFilters` and `Join` internals.
3. Re-run targeted test file until green:
   `mix test test/ecto_shorts/common_filters_test.exs`
4. Run full quality workflows:
   `/run-checks` then `/code-style-review`, repeat until both pass without fixes.
5. Run docs workflow `/write-docs` for changed modules and tests.
6. Write ADR in `docs/adr/` with next sequence number.

## Validation and Acceptance

Acceptance is reached when:

* Boundary tests prove invalid payloads no longer raise from `convert_params_to_filter/3`.
* Tests assert warnings are emitted and the query remains unchanged for failing operations.
* A test proves invalid join `:on` payload skips the join entry entirely.
* Existing valid query-building behavior remains green.
* `/run-checks` and `/code-style-review` both pass with no remaining issues.

## Idempotence and Recovery

The code changes are idempotent and can be re-applied safely by rerunning tests and the quality workflows. If a wrapper catches too broadly and hides legitimate developer errors, narrow exception handling and add assertion tests for the intended warning message to keep behavior explicit.

## Artifacts and Notes

Expected warning message shape examples (exact text may vary by module prefix):

    [EctoShorts.CommonFilters] Skipping filter :with_ties due to query-building error: ...
    [EctoShorts.CommonFilters.Join] Expected :on to be a keyword list, map, or true, got: ...

## Interfaces and Dependencies

Public boundary remains unchanged:

* `EctoShorts.CommonFilters.convert_params_to_filter/3 :: source, params, opts -> Ecto.Query.t()`

Internal interfaces to adjust:

* `EctoShorts.CommonFilters.build_query/6` should safely apply builder modules (`Filter`, `Distinct`, `Join`, etc.) and convert exceptions to warning+skip.
* `EctoShorts.CommonFilters.create_schema_filter/6` bind-path should not bubble `BindingParams` errors.
* `EctoShorts.CommonFilters.Join.on_expr/4` (or equivalent helper) should return skip signal for invalid payloads so the join entry is not applied.

Revision note: Initial ExecPlan created to capture approved behavior decisions and implementation sequence before code edits.

Revision note: Updated after implementation to reflect completed test-first workflow, final behavior decisions in code, successful quality checks, docs update, and ADR publication.
