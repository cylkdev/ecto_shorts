# Extract Focused Submodules from Actions, CommonFilters, and CommonParams

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The three largest modules in EctoShorts — `EctoShorts.Actions` (983 lines), `EctoShorts.CommonFilters` (727 lines), and `EctoShorts.CommonParams` (676 lines) — each contained multiple unrelated responsibilities bundled into a single file. This made it difficult to locate specific logic, increased merge-conflict risk, and forced unrelated concerns to change together.

This refactor extracts cohesive submodules from each, reducing per-module size and giving each module a single clear purpose. The existing public API entry points (`Actions.all`, `Actions.create`, `CommonFilters.convert_params_to_filter`, `CommonParams.convert_to_insert_params`, etc.) remain unchanged. All 645 tests continue to pass after every milestone.

After the refactor:

- `actions.ex` went from 983 to 614 lines (CRUD, find-and-X, bulk ops, transaction delegation).
- `common_filters.ex` went from 727 to 543 lines (param normalization, schema filter dispatch).
- `common_params.ex` went from 676 to 505 lines (insert/update param building, validation).
- Five new focused submodules were created.
- Duplicate `get_query_fields/2` was consolidated into `CommonSchema`.
- Near-identical `put_order_by`/`put_group_by` were deduplicated into a single `put_param/3`.
- 15 repetitive `apply_query_builder` clauses were replaced with map-based dispatch.

To verify behaviour is preserved, run `mix test` from the repository root. All 645 tests and 9 doctests must pass.

## Progress

- [x] (2026-02-28 11:52Z) Milestone 1A: Extracted `EctoShorts.Actions.Multi` — all multi-transaction helpers (build_*_many_multi, repo_*, handle_multi_response). 645 tests pass.
- [x] (2026-02-28 11:54Z) Milestone 1B: Extracted `EctoShorts.Actions.Batch` — all batch helpers (build_batch_params, normalize_batch_key, handle_batch_response, extract_lookup_params, etc.). 645 tests pass.
- [x] (2026-02-28 11:56Z) Milestone 1C: Deduplicated `put_order_by`/`put_group_by` into `put_param/3`. Consolidated `get_query_fields/2` into `CommonSchema`. Removed duplicate from `Actions` and `CommonParams`. 645 tests pass.
- [x] (2026-02-28 11:58Z) Milestone 2A: Replaced 15 `apply_query_builder` function clauses with `@query_builder_modules` map-based dispatch. Kept `:subquery` as special case. 645 tests pass.
- [x] (2026-02-28 12:00Z) Milestone 2B: Extracted `EctoShorts.CommonFilters.BindParams` — bind-param reduction chain. Promoted `reduce_filter_params` from `defp` to `@doc false def`. 645 tests pass.
- [x] (2026-02-28 12:02Z) Milestone 3A: Extracted `EctoShorts.CommonParams.Timestamps` — all timestamp logic (put_timestamps, put_set_updated_at, cast_datetime, truncate_datetime, timestamp_type). 645 tests pass.
- [x] (2026-02-28 12:04Z) Milestone 3B: Extracted `EctoShorts.CommonParams.Placeholders` — all placeholder logic (put_placeholders, on_placeholder_conflict). 645 tests pass.
- [x] (2026-02-28 12:05Z) Milestone 4A: Full quality check. `mix test` — 645 tests, 0 failures. `mix credo --strict` — passes (pre-existing warnings only). `mix dialyzer` — 1 pre-existing error in `dynamics.ex` unrelated to this refactor.

## Surprises & Discoveries

- Observation: The `@binding_selector_modes` module attribute in `CommonFilters` became unused after extracting `BindParams`. Removing it was required to avoid a compiler warning.
  Evidence: Compiler warning during `mix format && mix test` after Milestone 2B.

- Observation: The `@updated_at`, `@inserted_at`, `@utc_datetime`, and `@naive_datetime` module attributes in `CommonParams` became unused after extracting `Timestamps`. They were all removed.
  Evidence: Compiler warnings would have appeared on next compile.

- Observation: The dialyzer error `lib/ecto_shorts/dynamics.ex:1:pattern_match` is pre-existing and unrelated to this refactor. It was present before and after the changes.
  Evidence: `mix dialyzer` output on the unmodified codebase shows the same error.

## Decision Log

- Decision: Promote `reduce_filter_params` from `defp` to `@doc false def` in `CommonFilters` so that `BindParams` can call back into it.
  Rationale: `BindParams.reduce_binding_params` needs to dispatch back to the main filter reduction pipeline. Making it public with `@doc false` keeps it internal while avoiding circular module dependencies.
  Date/Author: 2026-02-28 / Cascade

- Decision: Keep `:subquery` as a separate `apply_query_builder` clause instead of including it in the `@query_builder_modules` map.
  Rationale: The `:subquery` handler has unique pre-filtering logic (it applies filters to the query before calling `SubQuery.build`) and a fallback clause for invalid params. This special behavior doesn't fit the uniform `module.build(...)` pattern.
  Date/Author: 2026-02-28 / Cascade

- Decision: Consolidate `get_query_fields/2` into `CommonSchema` rather than creating a new shared module.
  Rationale: `CommonSchema` already contains `get_schema_reflection/2` which `get_query_fields` delegates to. Adding it there is the natural home and avoids a new module for a single function.
  Date/Author: 2026-02-28 / Cascade

## Outcomes & Retrospective

The refactor achieved its goals. The three largest modules are now 37%, 25%, and 25% smaller respectively, with logic distributed into five focused submodules. No public API changed. All 645 tests pass.

Line count summary:

    Before:
      actions.ex          983
      common_filters.ex   727
      common_params.ex    676
      common_schema.ex    431
      Total              2817

    After:
      actions.ex          614  (-38%)
      common_filters.ex   543  (-25%)
      common_params.ex    505  (-25%)
      common_schema.ex    447  (+16, added get_query_fields/2)
      actions/multi.ex    244  (new)
      actions/batch.ex    124  (new)
      common_filters/bind_params.ex  145  (new)
      common_params/timestamps.ex    132  (new)
      common_params/placeholders.ex   43  (new)

Follow-up opportunities:

- `actions.ex` at 614 lines is still above the 400-line soft target. The CRUD functions, bulk functions, and transaction functions could be further separated in a future refactor.
- `common_params.ex` at 505 lines has a large `normalize_insert_entry` dispatch chain with many clauses that could benefit from further extraction.
- The pre-existing dialyzer error in `dynamics.ex` should be investigated separately.

## Context and Orientation

EctoShorts is an Elixir library that provides a standardized, data-driven API for working with Ecto. The key modules are:

- `lib/ecto_shorts/actions.ex` — Public CRUD, batch, bulk, multi, and transaction operations. The main entry point for users. All public functions remain here; internal helpers were extracted to `actions/multi.ex` and `actions/batch.ex`.
- `lib/ecto_shorts/common_filters.ex` — Converts parameter maps/keyword lists into Ecto queries. The `convert_params_to_filter/3` function is the entry point. Internally dispatches to query builder modules in `lib/ecto_shorts/common_filters/`. Binding-parameter reduction was extracted to `bind_params.ex`.
- `lib/ecto_shorts/common_params.ex` — Converts application-level data into Ecto-compatible insert/update structures. Timestamp and placeholder logic were extracted to `timestamps.ex` and `placeholders.ex`.
- `lib/ecto_shorts/common_schema.ex` — Schema introspection utilities. Now also houses the shared `get_query_fields/2`.

## Behaviour Boundary (Must Remain Unchanged)

- `EctoShorts.Actions.all/3` returns a list of schema structs matching the given params.
- `EctoShorts.Actions.find/3` returns `{:ok, record}` or `{:error, %ErrorMessage{code: :not_found}}`.
- `EctoShorts.Actions.create/3` returns `{:ok, record}` or `{:error, changeset}`.
- `EctoShorts.Actions.create_many/3` returns `{:ok, [records]}` or `{:error, %ErrorMessage{}}` and rolls back on failure.
- `EctoShorts.CommonFilters.convert_params_to_filter/3` returns an `Ecto.Query.t()`.
- `EctoShorts.CommonParams.convert_to_insert_params/3` returns `{:ok, [maps]}` or `{:error, [changesets]}`.
- `EctoShorts.CommonParams.convert_to_update_params/3` returns a keyword list of update operations.
- All 645 tests and 9 doctests pass with `mix test`.

## Code Smell Identified

Three code smells from the catalog:

1. **Large Module** (`.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`): All three target modules exceeded 500 lines, making them hard to navigate and understand. A large module indicates that multiple concerns have been bundled together.

2. **Divergent Change** (`.agent/refactor/code_smells/change_preventers/DIVERGENT_CHANGE.md`): `actions.ex` changed for CRUD reasons, batch reasons, multi-transaction reasons, and transaction-handling reasons — four unrelated axes of change in one file.

3. **Duplicate Code** (`.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`): `get_query_fields/2` was duplicated in both `Actions` and `CommonParams`. `put_order_by/2` and `put_group_by/2` were structurally identical functions differing only in the key name.

## Refactoring Technique Selected

1. **Extract Module** (`.agent/refactor/techniques/moving_features_between_modules/EXTRACT_MODULE.md`): Used to create `Actions.Multi`, `Actions.Batch`, `CommonFilters.BindParams`, `CommonParams.Timestamps`, and `CommonParams.Placeholders`. Each extracted module has one clear responsibility.

2. **Extract Function** (`.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`): Used to deduplicate `put_order_by`/`put_group_by` into a single parameterized `put_param/3` helper.

## Plan of Work

See the Progress section for the completed sequence of milestones.

## Concrete Steps

From the repository root:

    export PATH="$HOME/.asdf/shims:$PATH"
    mix format
    mix test
    mix credo --strict
    mix dialyzer

Expected: 645 tests, 0 failures. Credo passes. Dialyzer shows 1 pre-existing error in `dynamics.ex`.

## Validation and Acceptance

Run `mix test` and observe 645 tests, 9 doctests, 0 failures. The refactor preserves all existing tests. No new tests were needed because no behaviour changed.

## Idempotence and Recovery

All changes are additive extractions followed by call-site updates. Running the steps again produces the same result. If any step fails partway, `git checkout -- lib/` restores the original state.

## Artifacts and Notes

Test output after final milestone:

    Running ExUnit with seed: 764231, max_cases: 24
    ...(654 dots)...
    Finished in 0.9 seconds (0.6s async, 0.3s sync)
    9 doctests, 645 tests, 0 failures

## Interfaces and Dependencies

In `lib/ecto_shorts/actions.ex`, preserve all public `def` functions unchanged. Internal delegation now goes through:

    EctoShorts.Actions.Multi.build_create_many_multi/3
    EctoShorts.Actions.Multi.handle_multi_response/2
    EctoShorts.Actions.Batch.build_batch_params/4
    EctoShorts.Actions.Batch.normalize_batch_key/2

In `lib/ecto_shorts/common_filters.ex`, the public API is unchanged:

    EctoShorts.CommonFilters.convert_params_to_filter/3

One function was promoted from private to internal public:

    EctoShorts.CommonFilters.reduce_filter_params/6  (@doc false)

In `lib/ecto_shorts/common_schema.ex`, one function was added:

    EctoShorts.CommonSchema.get_query_fields/2

## Milestones

See the Progress section. All 8 milestones are complete. Each was independently verified with `mix format && mix test`.

## Revision Note

Initial completion — all milestones executed, validated, and retrospective written. 2026-02-28.
