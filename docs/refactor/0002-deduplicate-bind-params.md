# Deduplicate Bind-Parameter Dispatch Across Filter Submodules

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

Eight filter submodules under `lib/ecto_shorts/common_filters/` each contain a private `reduce_*_bind` function that implements the same ~40-line algorithm: validate that `:bind` params are a keyword list or map, normalize maps to keyword lists, validate that each key is a recognized binding mode (`:as` or `:at`), normalize each scoped payload, iterate `{binding_target, value}` pairs, and dispatch each pair back into the module's own reducer. The only difference between the eight copies is the callback function invoked on each pair.

This duplication means that any bug fix or behavior change to bind-parameter handling must be applied in eight places. It also inflates total line count by approximately 280 lines of near-identical code.

This refactor extracts the common algorithm into a single shared function in `EctoShorts.CommonFilters.BindingParams`, parameterized by a callback. Each submodule replaces its private copy with a one-line call to the shared function, passing a closure that dispatches to its own reducer. No public API changes. All existing tests continue to pass.

To verify behavior is preserved, run `mix test` from the repository root. All tests must pass after every milestone.

## Progress

- [x] (2026-02-28 12:50Z) Wrote RefactorPlan.
- [x] (2026-02-28 12:51Z) Milestone 1: Added `reduce_submodule_bind_params/3` to BindingParams. Tests pass (645 tests, 0 failures).
- [x] (2026-02-28 12:53Z) Milestone 2: Replaced duplicates in Distinct, GroupBy, WithTies, Windows. Tests pass (645 tests, 0 failures).
- [x] (2026-02-28 12:55Z) Milestone 3: Replaced duplicates in OrderBy, Update, Select, Preload. Tests pass (645 tests, 0 failures).
- [x] (2026-02-28 12:56Z) Milestone 4: Final validation. `mix format` clean. `mix test` - 645 tests, 0 failures. `mix credo --strict` - no new warnings (pre-existing only). Credo `mods/funs` count dropped from 749 to 735 (14 fewer function clauses).

## Surprises & Discoveries

- Observation: The `@binding_selector_modes` module attribute became unused in all 8 submodules after replacing the private bind functions. Removing it in each module was required to keep the code clean and avoid potential compiler warnings.
  Evidence: Each submodule previously defined `@binding_selector_modes [:as, :at]` solely for use in the `reduce_*_bind` guard clause `when binding_mode in @binding_selector_modes`. After replacement, the attribute is no longer referenced.

## Decision Log

- Decision: Add a new public function `reduce_submodule_bind_params/3` to the existing `BindingParams` module rather than creating a new module.
  Rationale: `BindingParams` already exists as the centralized bind-parameter handling module. Adding to it keeps the abstraction in one place. The function is `@doc false` since it is internal.
  Date/Author: 2026-02-28 / Cascade

- Decision: The callback signature is `callback.(query, {binding_mode, binding_target}, value)` returning the updated query.
  Rationale: This is the minimal interface each submodule needs. The binding_mode and binding_target are passed as a tuple so callers can construct their own binding_selector.
  Date/Author: 2026-02-28 / Cascade

## Outcomes & Retrospective

The refactor is complete. Eight near-identical `reduce_*_bind` private functions (each ~40 lines) were replaced with single-line calls to the new shared `BindingParams.reduce_submodule_bind_params/3`. This eliminated approximately 280 lines of duplicated code and reduced the credo `mods/funs` count from 749 to 735 (14 fewer function clauses).

The behavior boundary was fully preserved: all 645 tests and 9 doctests pass. No public API was changed. The same `ArgumentError` messages are raised for invalid bind params. `mix format` is clean. `mix credo --strict` shows no new warnings.

Files changed:
- `lib/ecto_shorts/common_filters/bind_params.ex` - added `reduce_submodule_bind_params/3`
- `lib/ecto_shorts/common_filters/distinct.ex` - replaced `reduce_distinct_bind/3` (removed ~40 lines)
- `lib/ecto_shorts/common_filters/group_by.ex` - replaced `reduce_group_by_bind/3` (removed ~40 lines)
- `lib/ecto_shorts/common_filters/order_by.ex` - replaced `reduce_order_by_bind/4` (removed ~40 lines)
- `lib/ecto_shorts/common_filters/preload.ex` - replaced `reduce_preload_bind/5` (removed ~50 lines)
- `lib/ecto_shorts/common_filters/select.ex` - replaced `reduce_select_bind/6` (removed ~40 lines)
- `lib/ecto_shorts/common_filters/update.ex` - replaced `reduce_update_bind/4` (removed ~40 lines)
- `lib/ecto_shorts/common_filters/windows.ex` - replaced `reduce_windows_bind/3` (removed ~40 lines)
- `lib/ecto_shorts/common_filters/with_ties.ex` - replaced `reduce_with_ties_bind/3` (removed ~40 lines)

Follow-up opportunity: The `Having` module does not use the `:bind` pattern at all (it uses dynamic expressions directly), so it was not affected by this refactor. If `:bind` support is added to `Having` in the future, it should use `BindingParams.reduce_submodule_bind_params/3` rather than adding a private copy.

## Context and Orientation

EctoShorts is an Elixir library that provides a data-driven API for Ecto query composition. The `lib/ecto_shorts/common_filters/` directory contains submodules that each handle one type of Ecto query operation (distinct, group_by, order_by, etc.). Each submodule implements a `build/6` public function that the main `CommonFilters` module dispatches to via a map-based lookup.

Most submodules support a `:bind` key in their params that allows callers to target specific query bindings (by name with `:as` or by position with `:at`). The handling of this `:bind` key follows an identical pattern across all eight modules, but each module has its own private copy.

The existing `EctoShorts.CommonFilters.BindingParams` module at `lib/ecto_shorts/common_filters/bind_params.ex` already handles bind-parameter dispatch for the top-level `CommonFilters` module. This refactor extends it with a reusable helper for submodules.

Key files:
- `lib/ecto_shorts/common_filters/bind_params.ex` - existing bind-param helper (will be extended)
- `lib/ecto_shorts/common_filters/distinct.ex` - has `reduce_distinct_bind/3`
- `lib/ecto_shorts/common_filters/group_by.ex` - has `reduce_group_by_bind/3`
- `lib/ecto_shorts/common_filters/order_by.ex` - has `reduce_order_by_bind/4`
- `lib/ecto_shorts/common_filters/preload.ex` - has `reduce_preload_bind/5`
- `lib/ecto_shorts/common_filters/select.ex` - has `reduce_select_bind/6`
- `lib/ecto_shorts/common_filters/update.ex` - has `reduce_update_bind/4`
- `lib/ecto_shorts/common_filters/windows.ex` - has `reduce_windows_bind/3`
- `lib/ecto_shorts/common_filters/with_ties.ex` - has `reduce_with_ties_bind/3`

## Behavior Boundary (Must Remain Unchanged)

All public functions in all affected modules must return identical results for identical inputs. Specifically:

- `EctoShorts.CommonFilters.convert_params_to_filter/3` must produce the same `Ecto.Query` for any combination of `:bind`, `:distinct`, `:group_by`, `:order_by`, `:preload`, `:select`, `:select_merge`, `:update`, `:windows`, and `:with_ties` params.
- All `ArgumentError` exceptions raised for invalid bind params must preserve the same error messages.
- The existing test suite (645+ tests) must pass without modification.

## Code Smell Identified

Duplicate Code, from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`. Identical or very similar code exists in 8 places. The same bind-parameter validation and dispatch algorithm is repeated with the only variation being the callback function name. Signs: copy-pasted code blocks across modules, similar functions that differ only in small details, bug fixes that must be applied in multiple places.

## Refactoring Technique Selected

Extract Function (`.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`) combined with Form Template Function (`.agent/refactor/techniques/dealing_with_generalization/FORM_TEMPLATE_FUNCTION.md`). The common algorithm is extracted into a single shared function. The variable step (dispatching to the module's own reducer) is parameterized as a callback function argument. This preserves behavior because the shared function performs identical validation and iteration, and the callback preserves each module's dispatch semantics.

## Plan of Work

The work proceeds in four milestones. Each milestone is independently verifiable.

Milestone 1 adds the shared helper function to `BindingParams`. Milestones 2 and 3 replace the duplicated private functions in the eight submodules, split into two batches for manageable diffs. Milestone 4 runs the full quality check suite.

## Concrete Steps

All commands run from the repository root: `/Users/kurthogarth/Documents/GitHub/ecto_shorts`

    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix format
    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix test
    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix credo --strict

## Validation and Acceptance

Run `mix test` after every milestone. All tests must pass. The refactor introduces no new tests because it is purely structural - the existing tests already exercise all bind-parameter paths through integration. If any test fails, the milestone must be rolled back and investigated before proceeding.

Run `mix credo --strict` at Milestone 4. No new warnings should appear.

## Idempotence and Recovery

Each milestone is a set of file edits. If a milestone partially fails, revert the changed files with `git checkout -- <file>` and retry. The milestones are ordered so that Milestone 1 is additive (adds code, changes nothing), and Milestones 2-3 are substitutive (replace private functions with calls to the shared helper). This means Milestone 1 can be run independently and tests will still pass with the old code in place.

## Artifacts and Notes

(To be filled during execution.)

## Interfaces and Dependencies

In `lib/ecto_shorts/common_filters/bind_params.ex`, the new function:

    @doc false
    def reduce_submodule_bind_params(query, bind_params, callback)

Where `callback` is `(query, {binding_mode, binding_target}, value) -> query`.

All existing public functions in `BindingParams` remain unchanged. All `defp` functions in the eight submodules that are being replaced are private and have no external callers.

## Milestones

### Milestone 1: Add shared helper to BindingParams

Add `reduce_submodule_bind_params/3` to `lib/ecto_shorts/common_filters/bind_params.ex`. This function implements the common bind-parameter validation and dispatch algorithm, taking a callback for the variable step. No existing code is modified. Run `mix format` and `mix test`. All tests pass because the new function is not yet called.

### Milestone 2: Replace duplicates in batch 1 (Distinct, GroupBy, WithTies, Windows)

In each of these four modules, delete the private `reduce_*_bind` function clauses and replace the call site with a call to `BindingParams.reduce_submodule_bind_params/3`, passing a closure that calls the module's own reducer. Remove the now-unused `@binding_selector_modes` module attribute if it becomes unused. Run `mix format` and `mix test` after each file.

### Milestone 3: Replace duplicates in batch 2 (OrderBy, Update, Select, Preload)

These four modules have slightly different signatures (extra `filter_op`, `opts`, `schema` args in the bind function). The callback closure captures these extra values from the enclosing scope. Same approach as Milestone 2. Run `mix format` and `mix test` after each file.

### Milestone 4: Final validation

Run `mix format`, `mix test`, and `mix credo --strict`. All must pass. Update Progress and Outcomes sections.
