# Simplify WithNamedBinding for Readability

This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The `apply_entry` function in `lib/ecto_shorts/common_filters/with_named_binding.ex` had three levels of nested `if` statements that forced a reader to hold multiple conditions in their head at once. The `reduce_entries` function had a standalone tuple clause that existed only to serve an indirect recursion path from the non-keyword list branch. After this refactor, `apply_entry` uses a flat `cond` with early exits, and the non-keyword list branch dispatches directly to `apply_entry` via pattern-matching in the `Enum.reduce` callback. The module now reads top-to-bottom with minimal nesting.

## Progress

- [x] (2026-02-28) Flattened triple-nested `if` in `apply_entry` to `cond`.
- [x] (2026-02-28) Inlined standalone tuple clause into non-keyword list branch of `reduce_entries`.
- [ ] Run focused tests and full test suite; record evidence and finalize retrospective.

## Surprises & Discoveries

None so far.

## Decision Log

- Decision: Use `cond` instead of nested `if` in `apply_entry`.
  Rationale: `cond` makes the decision tree scannable as a flat list of conditions. The first two branches (`not Keyword.keyword?` and `has_named_binding?`) are early exits that reject or short-circuit. The third branch (`true`) is the happy path. This reduces max nesting from 3 levels to 1.
  Date/Author: 2026-02-28

- Decision: Keep the catch-all `reduce_entries(query, value, _opts)` clause even after inlining the tuple clause.
  Rationale: The catch-all handles the case where `reduce_entries` is called with a non-list, non-map, non-tuple value directly from `build/6`. Removing it would leave that path unhandled.
  Date/Author: 2026-02-28

## Outcomes & Retrospective

Pending test verification. The `apply_entry` keyword-list clause went from 22 lines with 3 nesting levels to 23 lines with 1 nesting level. The standalone tuple `reduce_entries` clause was removed and its logic inlined into the non-keyword list branch's `Enum.reduce` callback via pattern matching.

## Context and Orientation

`lib/ecto_shorts/common_filters/with_named_binding.ex` defines `EctoShorts.CommonFilters.WithNamedBinding`, which builds `:with_named_binding` query expressions. It has two private function groups:

- `reduce_entries/3` - normalises input (maps to keyword lists) and iterates over entries, calling `apply_entry` for each.
- `apply_entry/4` - validates a single `{binding_key, binding_params}` entry, checks if the named binding already exists, applies filters, and verifies the binding was created.

## Behavior Boundary (Must Remain Unchanged)

The public `build/6` function must produce identical results for all existing test inputs. The 6 tests in `describe "convert_params_to_filter/3 with_named_binding"` (query_operation_test.exs) cover: missing binding creation, no-op when binding exists, multiple entries, map payload, invalid key type, invalid params type, and missing binding callback result.

## Code Smell Identified

`Long Function` (from `.agent/refactor/code_smells/bloaters/LONG_FUNCTION.md`). The `apply_entry` clause at lines 58–81 had deep nesting of `if` expressions (3 levels), making it difficult to scan the decision tree at a glance. Deep nesting is a sign that a function handles multiple distinct conditions inline rather than separating them.

## Refactoring Technique Selected

Two techniques were applied:

1. Replace nested conditional with flat `cond` - a variant of "Replace Nested Conditional with Guard Clauses" where the flat `cond` serves the same purpose as guard-style early exits.

2. `Inline Function` (from `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`) - the standalone tuple clause `reduce_entries(query, {binding_key, binding_params}, opts)` only forwarded to `apply_entry` and was called from exactly one site (the non-keyword list reduce). It was inlined into that reduce callback's pattern match.

## Plan of Work

1. In `apply_entry` (keyword-list clause), replace the triple-nested `if` with a `cond` that tests conditions in order: invalid keyword list, binding already exists, then the happy path.

2. In `reduce_entries` (list clause), replace the non-keyword-list branch's indirect recursion with a multi-head `fn` that pattern-matches tuples directly to `apply_entry` and warns for non-tuple entries.

3. Remove the standalone `reduce_entries(query, {binding_key, binding_params}, opts)` clause since it is no longer called.

## Concrete Steps

From the repository root:

    mix format
    mix test test/ecto_shorts/common_filters/query_operation_test.exs --seed 0
    mix test

## Validation and Acceptance

Run `mix test` from the repository root. All existing tests must pass. The 6 with_named_binding tests in query_operation_test.exs must produce identical results.

## Idempotence and Recovery

The changes are safe to apply multiple times. Reverting `lib/ecto_shorts/common_filters/with_named_binding.ex` restores the original behavior.

## Artifacts and Notes

Before `apply_entry` (3 nesting levels):

    if Keyword.keyword?(binding_params) do
      if Query.has_named_binding?(query, binding_key) do
        query
      else
        new_query = ...
        if Query.has_named_binding?(new_query, binding_key) do ...

After `apply_entry` (1 nesting level):

    cond do
      not Keyword.keyword?(binding_params) ->
        warn + return query
      Query.has_named_binding?(query, binding_key) ->
        return query
      true ->
        new_query = ...
        if Query.has_named_binding?(new_query, binding_key) do ...

## Interfaces and Dependencies

No public API changes. Only private functions were modified:

- `defp reduce_entries/3` - removed standalone tuple clause, inlined into list clause
- `defp apply_entry/4` - replaced nested `if` with `cond` in the keyword-list clause

## Milestones

### Milestone 1: Flatten apply_entry and inline tuple clause

Goal: Replace triple-nested `if` with `cond` and remove indirect dispatch through `reduce_entries` for tuple entries.

Files changed:
- `lib/ecto_shorts/common_filters/with_named_binding.ex`

Commands:

    mix format
    mix test test/ecto_shorts/common_filters/query_operation_test.exs --seed 0
    mix test

Expected outcome: All tests pass. No warnings.
