# Simplify Three-Way Case Split in Distinct, GroupBy, and OrderBy Reducers

This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The keyword-list handler in `reduce_params` (Distinct), `reduce_group_by` (GroupBy), and `reduce_order_by` (OrderBy) each contained a three-way `case` over `Enum.split_with` that separated bind entries from operation entries. All three branches did the same work — process operation entries then bind entries — but the `{[], ...}` and `{..., []}` branches were unnecessary special cases of the general branch, because `Enum.reduce` over an empty list is a no-op and passing an empty list to `apply_*_expr` is harmless. Additionally, in Distinct and GroupBy, operation keyword entries were reduced one-by-one through the reducer function back to `apply_*_expr`, when `apply_*_expr` already handles lists directly.

After this refactor, all three modules use a single path: split, pass operation entries as a list if non-empty, then reduce bind entries. The behaviour is unchanged.

## Progress

- [x] (2026-02-28) Identified the three-way case pattern in all three modules.
- [x] (2026-02-28) Simplified `reduce_params` keyword-list branch in `lib/ecto_shorts/common_filters/distinct.ex`.
- [x] (2026-02-28) Simplified `reduce_group_by` keyword-list branch in `lib/ecto_shorts/common_filters/group_by.ex`.
- [x] (2026-02-28) Simplified `reduce_order_by` keyword-list branch in `lib/ecto_shorts/common_filters/order_by.ex`.
- [ ] Run focused tests and full test suite; record evidence and finalize retrospective.

## Surprises & Discoveries

- Observation: The Distinct module's catch-all `apply_distinct_expr` was updated separately (outside this refactor) to log a warning and return the query unchanged, rather than passing the expression through to `Query.distinct/2`. This does not affect the refactoring since the keyword-list handler never reaches the catch-all — it passes lists to the list-specific clause.

## Decision Log

- Decision: Collapse three-way `case` into a single split-and-dispatch path.
  Rationale: All three branches performed the same work. `Enum.reduce/3` over an empty list returns the accumulator unchanged, so the `{[], ...}` and `{..., []}` branches added no value.
  Date/Author: 2026-02-28

- Decision: Keep the single-tuple passthrough clause (e.g. `reduce_params(query, binding_selector, {key, value})`) in all three modules.
  Rationale: This clause handles the map-to-keyword-list conversion path and non-bind tuple dispatch. Removing it would change the dispatch shape and risk breaking the recursive normalisation flow.
  Date/Author: 2026-02-28

- Decision: Guard operation entries dispatch with `if entries != []` rather than always calling `apply_*_expr(query, binding_selector, [])`.
  Rationale: While Ecto's `distinct/2`, `group_by/2`, and `order_by/2` tolerate empty lists, explicitly guarding avoids appending empty query clauses and keeps the generated SQL clean.
  Date/Author: 2026-02-28

## Outcomes & Retrospective

Pending test verification. The refactoring removed approximately 10 lines per module (30 total) by collapsing three branches into one. The resulting code matches a consistent pattern across all three modules.

## Context and Orientation

Three sibling modules under `lib/ecto_shorts/common_filters/` build Ecto query expressions from data-driven params:

- `distinct.ex` — `EctoShorts.CommonFilters.Distinct`, builds `:distinct` expressions
- `group_by.ex` — `EctoShorts.CommonFilters.GroupBy`, builds `:group_by` expressions
- `order_by.ex` — `EctoShorts.CommonFilters.OrderBy`, builds `:order_by` / `:prepend_order_by` expressions

Each module has a private reducer function that normalises input (maps to keyword lists, separates `:bind` entries from operation entries) and dispatches to an `apply_*_expr` function that builds the actual Ecto query clause. The `apply_*_expr` functions are generated at compile time by `EctoShorts.Compiler.define_clauses` to handle different binding selectors.

The "three-way case" refers to the pattern where `Enum.split_with` produces `{bind_entries, operation_entries}`, and the code matched on three shapes: only operation entries, only bind entries, or both. All three shapes performed the same two-step reduce.

## Behaviour Boundary (Must Remain Unchanged)

The public `build/6` function in each module must produce identical `Ecto.Query` structs for all existing test inputs. Specifically:

- `EctoShorts.CommonFilters.Distinct.build/6` — boolean, atom, list, `{dir, field}`, map, and `:bind`-scoped payloads
- `EctoShorts.CommonFilters.GroupBy.build/6` — atom, list, map, and `:bind`-scoped payloads
- `EctoShorts.CommonFilters.OrderBy.build/6` — atom, `{dir, field}`, list, map, dynamic, and `:bind`-scoped payloads for both `:order_by` and `:prepend_order_by`

## Code Smell Identified

`Duplicate Code` (from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`). The three-way `case` pattern was structurally identical across all three modules and within each module the three branches performed the same work with only the empty-list edge cases differing. This is "structural duplication" — the same algorithm with slight variations that add no value.

## Refactoring Technique Selected

`Inline Function` (from `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`). The two special-case branches were inlined into the general case by recognising that `Enum.reduce` over an empty list is a no-op. No new functions were introduced; two branches were removed.

## Plan of Work

For each of the three modules, replace the `case Enum.split_with(...)` with three branches with a single path:

1. Bind `{bind_entries, operation_entries}` from `Enum.split_with`.
2. If `operation_entries` is non-empty, pass it as a list to `apply_*_expr`.
3. Reduce `bind_entries` through the reducer function.

## Concrete Steps

From the repository root:

    mix format
    mix test test/ecto_shorts/common_filters/query_operation_test.exs test/ecto_shorts/common_filters/binding_and_boolean_test.exs --seed 0
    mix test

## Validation and Acceptance

Run `mix test` from the repository root. All existing tests must pass with no failures. The distinct tests (in `test/ecto_shorts/common_filters/query_operation_test.exs`, describe block "convert_params_to_filter/3 distinct") and order_by/group_by tests in the same file must produce identical SQL assertions.

## Idempotence and Recovery

The changes are safe to apply multiple times. If any test fails, reverting the three edited files restores the original behaviour.

## Artifacts and Notes

Before (all three modules, ~22 lines):

    case Enum.split_with(entries, fn {k, _} -> k === @binding_selector_key end) do
      {[], operation_entries} ->
        # reduce operation entries individually
      {bind_entries, []} ->
        # reduce bind entries individually
      {bind_entries, operation_entries} ->
        # reduce operation entries individually, then bind entries
    end

After (all three modules, ~10 lines):

    {bind_entries, operation_entries} =
      Enum.split_with(entries, fn {k, _} -> k === @binding_selector_key end)

    query =
      if operation_entries != [],
        do: apply_*_expr(query, binding_selector, operation_entries),
        else: query

    Enum.reduce(bind_entries, query, fn entry, query_acc ->
      reduce_*(query_acc, binding_selector, entry)
    end)

## Interfaces and Dependencies

No public API changes. Only private functions were modified:

- `EctoShorts.CommonFilters.Distinct` — `defp reduce_params/3`
- `EctoShorts.CommonFilters.GroupBy` — `defp reduce_group_by/3`
- `EctoShorts.CommonFilters.OrderBy` — `defp reduce_order_by/4`

## Milestones

### Milestone 1: Apply simplification to all three modules

Goal: Replace three-way case with single-path dispatch in `distinct.ex`, `group_by.ex`, and `order_by.ex`.

Files changed:
- `lib/ecto_shorts/common_filters/distinct.ex`
- `lib/ecto_shorts/common_filters/group_by.ex`
- `lib/ecto_shorts/common_filters/order_by.ex`

Commands:

    mix format
    mix test test/ecto_shorts/common_filters/query_operation_test.exs test/ecto_shorts/common_filters/binding_and_boolean_test.exs --seed 0

Expected outcome: All tests pass. No warnings.

### Milestone 2: Full suite verification

Goal: Confirm no regressions across the entire test suite.

Commands:

    mix test

Expected outcome: All tests pass (same count as before the refactor).
