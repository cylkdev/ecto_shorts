# Deduplicate BindingParams Internal Logic and Unify Callers

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

`lib/ecto_shorts/common_filters/binding_params.ex` contained two public function groups that implemented nearly identical input-validation and entry-dispatch logic. `build_binding_params/6` (3 clauses, ~66 lines) validated raw bind params and dispatched each entry via `apply_flat_bind_entry/5`. `normalize_bind_params/2` (4 clauses, ~46 lines) performed the same validation with the same guards, branches, and warning messages, dispatching each entry via `normalize_flat_entry/2`. Their private helpers mirrored each other too: both popped `:as`/`:at`, called `resolve_at_target`, and handled missing keys with the same warning text.

The only difference was that `build_binding_params/6` applied each entry to the query (calling `CommonFilters.create_schema_filter/6` with a `max_binding_positions` guard), while `normalize_bind_params/2` returned `{binding_selector, value}` tuples for the caller to reduce over.

The caller picture reinforced the duplication: `common_filters.ex` called `build_binding_params/6` (normalize + apply in one step), while 7 submodules (Distinct, GroupBy, OrderBy, Preload, Select, Update, Windows) called `normalize_bind_params/2` then reduced over results.

After this refactor, `binding_params.ex` shrank from 288 lines to 138 lines. `build_binding_params/6` and its 4 private helpers were deleted. `common_filters.ex` calls `normalize_bind_params/2` + `Enum.reduce`, matching the pattern already used by the 7 submodules. All callers now use one shared normalization path.

To verify behavior is preserved, run `mix test --seed 0 --trace` from the repository root. All tests must pass after every milestone.

## Progress

- [x] (2026-03-02 03:56Z) Wrote RefactorPlan at `.windsurf/plans/refactor-binding-params-529014.md`.
- [x] (2026-03-02 04:00Z) Milestone 1: Rewrote `build_binding_params/6` to delegate to `normalize_bind_params/2`. Deleted 4 private helpers. Tests pass (9 doctests, 898 tests, 0 failures).
- [x] (2026-03-02 04:02Z) Milestone 2: Inlined reduce into `common_filters.ex`. Deleted `build_binding_params/6` from `binding_params.ex`. Removed unused aliases. Updated moduledoc. Tests pass (9 doctests, 898 tests, 0 failures).
- [x] (2026-03-02 04:04Z) Milestone 3: Final validation. `mix format --check-formatted` clean. `mix test --seed 0 --trace` passes (9 doctests, 898 tests, 0 failures). `mix credo --strict` passes with no new warnings (pre-existing only). `mix dialyzer` passes with no new warnings (2 pre-existing `pattern_match_cov` warnings).

## Surprises & Discoveries

- Observation: The `as: nil` edge case required careful handling. `normalize_flat_entry/2` returns `{{:as, nil}, []}` for both error sentinels (missing `:as`/`:at` key, invalid `:at` target) and for a user who passes `as: nil`. The reduce callback distinguishes them by checking whether the value is empty (`[]`) or not.
  Evidence: The reduce uses `{{:as, nil}, []}` to skip silently (error already warned by `normalize_flat_entry`) and `{{:as, nil}, _value}` to emit the "non-nil atom" warning for a real `as: nil` input.

- Observation: `Config` was not aliased in `common_filters.ex`. Adding `alias EctoShorts.Config` was required for the `max_binding_positions` check in the inlined reduce.

## Decision Log

- Decision: Preserve the `@binding_params_prefix` as `"EctoShorts.CommonFilters.BindingParams"` in `common_filters.ex` for warning messages that originated from `BindingParams`.
  Rationale: The `max_binding_positions` and `:as` validation warnings are conceptually BindingParams concerns. Using the original prefix avoids changing log output that users may filter on.
  Date/Author: 2026-03-02 / Cascade

- Decision: Distinguish `{{:as, nil}, []}` (error sentinel) from `{{:as, nil}, value}` (user-passed nil alias) in the reduce callback rather than modifying `normalize_flat_entry`.
  Rationale: Modifying `normalize_flat_entry` would change behavior for the 7 submodule callers that also use `normalize_bind_params`. The reduce-side check preserves exact behavior for all paths.
  Date/Author: 2026-03-02 / Cascade

## Outcomes & Retrospective

The refactor is complete. `binding_params.ex` shrank from 288 lines to 138 lines (52% reduction). The duplicate input-validation algorithm (~140 lines across `build_binding_params/6` and its 4 private helpers) was eliminated. `common_filters.ex` now calls `BindingParams.normalize_bind_params/2` + `Enum.reduce`, matching the pattern already used by 7 submodules.

The behavior boundary was fully preserved: all 898 tests and 9 doctests pass. No public API was changed. Warning messages preserve the same prefix and content. `mix format`, `mix credo --strict`, and `mix dialyzer` show no new issues.

Files changed:

- `lib/ecto_shorts/common_filters/binding_params.ex` - deleted `build_binding_params/6` and 4 private helpers (`apply_flat_bind_entry/5`, `apply_as_binding/6`, `apply_at_binding/6`). Removed unused aliases (`CommonFilters`, `Config`). Updated moduledoc.
- `lib/ecto_shorts/common_filters.ex` - replaced `build_binding_params` call with `normalize_bind_params` + reduce. Added `alias EctoShorts.Config` and `@binding_params_prefix`.

Follow-up opportunity: `with_ties.ex` (lines 38-80) has its own inline bind-params handling and its own `resolve_at_target/2` (lines 121-131). A separate refactor could make it use `normalize_bind_params` with a custom reduce callback.

## Context and Orientation

EctoShorts is an Elixir library that provides a data-driven API for Ecto query composition. The `lib/ecto_shorts/common_filters/` directory contains submodules that each handle one type of Ecto query operation (distinct, group_by, order_by, etc.).

Most submodules support a `:bind` key in their params that allows callers to target specific query bindings (by name with `:as` or by position with `:at`). The handling of this `:bind` key is centralized in `EctoShorts.CommonFilters.BindingParams` at `lib/ecto_shorts/common_filters/binding_params.ex`.

Before this refactor, `BindingParams` had two parallel code paths:

1. `build_binding_params/6` - called from `CommonFilters.create_schema_filter/6` for top-level `:bind` params. It validated input, normalized entries, and applied each to the query by calling back into `create_schema_filter/6`.
2. `normalize_bind_params/2` - called from 7 submodules (Distinct, GroupBy, OrderBy, Preload, Select, Update, Windows). It validated input with identical logic and returned `[{binding_selector, value}]` tuples.

Key files:

- `lib/ecto_shorts/common_filters/binding_params.ex` - the refactored module.
- `lib/ecto_shorts/common_filters.ex` - the main dispatch module that now contains the inlined reduce.

## Behavior Boundary (Must Remain Unchanged)

All public functions in all affected modules must return identical results for identical inputs.

- `EctoShorts.CommonFilters.convert_params_to_filter/3` must produce the same `Ecto.Query` for any combination of `:bind` params with `:as` or `:at` selectors.
- All warning messages for invalid bind params must preserve the same content.
- `BindingParams.normalize_bind_params/2` must return the same `[{binding_selector, value}]` tuples for the same inputs.
- The existing test suite (898 tests, 9 doctests) must pass without modification.

## Code Smell Identified

Duplicate Code, from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`. The same input-validation algorithm was implemented twice in the same module. `build_binding_params/6` and `normalize_bind_params/2` had identical guards, branches, and warning messages for handling single maps, keyword lists with `:as`/`:at`, lists of maps/keywords, and invalid input. Their private helpers (`apply_flat_bind_entry/5` and `normalize_flat_entry/2`) also mirrored each other. The only variation was whether the result was applied to a query or returned as data.

Signs: copy-pasted code blocks within the same module, similar functions that differ only in their final action (apply vs return), warning messages that must be kept in sync across both paths.

## Refactoring Technique Selected

Extract Function (`.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`) combined with Inline Function (`.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`). The shared normalization logic already existed as `normalize_bind_params/2`. The duplicate code in `build_binding_params/6` was rewritten to call it (Extract pattern - reuse existing extraction). Then the thin `build_binding_params/6` wrapper was inlined into its single call site in `common_filters.ex` (Inline). This preserved behavior because the shared function performs identical validation and the reduce callback preserves the original dispatch semantics.

## Plan of Work

The work proceeded in three milestones.

Milestone 1 rewrote `build_binding_params/6` to delegate to `normalize_bind_params/2` + reduce, eliminating the internal duplication. Milestone 2 inlined the reduce into `common_filters.ex` and deleted `build_binding_params/6`, unifying the caller pattern. Milestone 3 ran the full quality check suite.

## Concrete Steps

All commands run from the repository root: `/Users/kurthogarth/Documents/GitHub/ecto_shorts`

    mix format
    mix test --seed 0 --trace
    mix credo --strict
    mix dialyzer

## Validation and Acceptance

Run `mix test --seed 0 --trace` after every milestone. All 898 tests and 9 doctests must pass. The refactor introduces no new tests because it is purely structural - the existing tests already exercise all bind-parameter paths through integration.

Run `mix credo --strict` and `mix dialyzer` at Milestone 3. No new warnings should appear.

## Idempotence and Recovery

Each milestone is a set of file edits. If a milestone partially fails, revert the changed files with `git checkout -- <file>` and retry. Milestone 1 was additive in spirit (rewrote internals, kept the public function). Milestone 2 was substitutive (moved logic to caller, deleted the public function).

## Artifacts and Notes

Baseline test count: 9 doctests, 898 tests, 0 failures.
Post-refactor test count: 9 doctests, 898 tests, 0 failures.
Lines removed from `binding_params.ex`: ~150 (288 to 138).

## Interfaces and Dependencies

In `lib/ecto_shorts/common_filters/binding_params.ex`, the remaining public function:

    @doc false
    def normalize_bind_params(bind_params, query \\ nil)

Returns `[{binding_selector, value}]` where `binding_selector` is `{:as, atom}` or `{:at, integer}`, and `value` is a keyword list of filters or the extracted `:value` key content.

In `lib/ecto_shorts/common_filters.ex`, the `create_schema_filter/6` clause matching `{@binding_selector_key, bind_params}` now calls `BindingParams.normalize_bind_params/2` directly and reduces over the results with alias/position validation and `max_binding_positions` checking.

## Milestones

### Milestone 1: Rewrite `build_binding_params/6` to delegate to `normalize_bind_params/2`

Replaced the 3 `build_binding_params/6` clauses and 4 private helpers (`apply_flat_bind_entry/5`, `apply_as_binding/6`, `apply_at_binding/6`) with a single clause that calls `normalize_bind_params/2` then reduces with validation. Run `mix format` and `mix test --seed 0 --trace`. All 898 tests pass because the reduce callback preserves the exact validation and dispatch behavior.

### Milestone 2: Inline into `common_filters.ex` and delete `build_binding_params/6`

Moved the normalize+reduce logic from `build_binding_params/6` into the `create_schema_filter/6` clause in `common_filters.ex`. Added `alias EctoShorts.Config` and `@binding_params_prefix` to `common_filters.ex`. Deleted `build_binding_params/6` and unused aliases from `binding_params.ex`. Updated the moduledoc. Run `mix format` and `mix test --seed 0 --trace`. All 898 tests pass.

### Milestone 3: Final validation

Run `mix format --check-formatted`, `mix test --seed 0 --trace`, `mix credo --strict`, and `mix dialyzer`. All pass with no new issues. Updated Progress and Outcomes sections.
