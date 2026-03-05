# Consolidate Postgres adapter dispatch into cond blocks

This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

Three private functions in the Postgres adapter (`do_flatten/2`, `flatten_expression_params/1`, `flatten_operator_expr/1`) used multiple function clauses with guard expressions to dispatch on key identity and value type. The rest of the codebase — specifically `build_schema_filters/6` in `lib/ecto_shorts/common_filters.ex` — uses a single function clause per structural shape with a `cond` block inside for key-identity and value-type routing. This refactor aligns the Postgres adapter with that established pattern so a contributor reading one module recognises the same dispatch style in the other.

After the refactor, each function uses one clause per structural shape (tuple vs everything-else) and a `cond` block inside that clause for further routing. The `cond` ordering matches `build_schema_filters` exactly: map check → key identity check → keyword list check → catch-all. No public API, return values, or dynamic expression outputs change.

## Progress

- [x] (2026-03-04) Established green baseline: 9 doctests, 884 tests, 0 failures (seed 0).
- [x] (2026-03-04) Refactored `flatten_expression_params/1` from 4 guarded clauses to 1 tuple clause + 1 cond clause.
- [x] (2026-03-04) Refactored `do_flatten/2` from 8 guarded clauses to 1 tuple clause + 1 cond clause.
- [x] (2026-03-04) Refactored `flatten_operator_expr/1` from 3 guarded clauses to 1 cond clause.
- [x] (2026-03-04) Validated: 9 doctests, 884 tests, 0 failures (seed 0).
- [x] (2026-03-04) Wrote RefactorPlan.

## Surprises & Discoveries

- Observation: No surprises. The refactor was mechanical and all tests passed on the first run after the edit.

## Decision Log

- Decision: Keep `flatten_expression_params({op, value}) when is_atom(op)` as a separate function clause rather than folding it into the cond.
  Rationale: `common_filters.ex` keeps `{key, value}` tuple matching as a separate function clause from the catch-all `params` clause. The `when is_atom(op)` guard is structural (ensures the first element is an atom), not a key-identity check.
  Date/Author: 2026-03-04 / agent

- Decision: Use `params == []` inside a cond branch instead of a separate `do_flatten([], acc)` function clause.
  Rationale: `common_filters.ex` `apply_filters` catch-all puts type checks in `cond`, not in separate clauses. Empty list is a value-type check.
  Date/Author: 2026-03-04 / agent

- Decision: Follow map → boundary → keyword → catch-all ordering in `do_flatten({k, v}, acc)`.
  Rationale: Matches `build_schema_filters` ordering exactly. For boundary keys with map values, the map check fires first (converts to list, recurses), then the boundary check catches the keyword-list form on the next pass. End result is identical.
  Date/Author: 2026-03-04 / agent

## Outcomes & Retrospective

The refactor is complete. All three functions now follow the `cond`-block dispatch pattern from `common_filters.ex`. The full test suite (884 tests + 9 doctests) passes with zero failures and zero new warnings. No public interfaces changed.

## Context and Orientation

The file `lib/ecto_shorts/dynamics/adapters/postgres.ex` defines `EctoShorts.Dynamics.Postgres`, the PostgreSQL-specific dynamic expression adapter. It has one public callback `build_dynamic/4` and several private helpers that flatten nested expression maps/keyword-lists into a list of `{operator, value}` tuples before passing them to the `ScalarExpr`, `ArrayExpr`, or `CommonExpr` sub-modules.

The reference module is `lib/ecto_shorts/common_filters.ex` (`EctoShorts.CommonFilters`). Its `build_schema_filters/6` function (lines 1254–1305) is the canonical example of the dispatch pattern: one function clause matching `{key, value}` with a `cond` block inside, ordered map → key identity → keyword → catch-all.

The test file is `test/ecto_shorts/query_builder/dynamics/adapters/postgres_test.exs` (1077 lines, covering all operator types, array/scalar dispatch, arithmetic, aggregates, etc.).

## Behavior Boundary (Must Remain Unchanged)

`Postgres.build_dynamic/4` must return the same `Ecto.Query.DynamicExpr` structs for all inputs tested in `postgres_test.exs`. The 884 existing tests and 9 doctests are the behavior boundary.

## Code Smell Identified

Switch Statements (from `.agent/refactor/code_smells/abstraction_abusers/SWITCH_STATEMENTS.md`). The three functions used multiple function clauses with overlapping guard conditions to dispatch on key identity (`k in @instruction_boundary_operators`) interleaved with value-type guards (`is_map`, `is_list`). This spread the dispatch logic across many clauses, making it harder to read the full decision tree at a glance and inconsistent with the rest of the codebase.

## Refactoring Technique Selected

Inline Function (from `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`). The multiple guarded clauses were inlined into fewer clauses, each containing a `cond` block. No new functions were introduced; the logic was consolidated.

## Plan of Work

For each of the three functions, the multi-clause guard-based dispatch was replaced with a single (or two, for tuple+catch-all) clause containing a `cond` block. The `cond` ordering matches `build_schema_filters`: map check → key identity → keyword → catch-all.

## Concrete Steps

All commands run from the repository root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

1. Run baseline tests:

       mix test --seed 0 --trace

   Expected: 9 doctests, 884 tests, 0 failures.

2. Edit `lib/ecto_shorts/dynamics/adapters/postgres.ex`:
   - Replace `flatten_expression_params/1` (4 clauses → 2 clauses with cond)
   - Replace `do_flatten/2` (8 clauses → 2 clauses with cond)
   - Replace `flatten_operator_expr/1` (3 clauses → 1 clause with cond)

3. Run validation tests:

       mix test --seed 0 --trace

   Expected: 9 doctests, 884 tests, 0 failures.

## Validation and Acceptance

Run `mix test --seed 0 --trace` from the repository root. Expect 9 doctests, 884 tests, 0 failures. The refactored functions produce identical dynamic expression outputs for every input exercised by the test suite.

## Idempotence and Recovery

The edit is a pure replacement of private function bodies. Running the edit again on the already-refactored file is a no-op. If any step fails, `git checkout lib/ecto_shorts/dynamics/adapters/postgres.ex` restores the original.

## Artifacts and Notes

Test output after refactoring:

    Finished in 2.0 seconds (2.0s async, 0.00s sync)
    9 doctests, 884 tests, 0 failures
    Randomized with seed 0

## Interfaces and Dependencies

No public interfaces changed. The only public functions in the module are:

    @impl true
    def operators, do: @operators

    @impl true
    def operator?(key), do: key in @operators

    @impl true
    def build_dynamic(source, binding_selector, key, expr)

All three retain their existing signatures and return shapes. The refactored functions are all `defp`.

## Milestones

This refactor was small enough to complete in a single milestone.

### Milestone 1: Consolidate all three functions

Goal: Replace guard-based multi-clause dispatch with cond-block dispatch in `do_flatten/2`, `flatten_expression_params/1`, and `flatten_operator_expr/1`.

Files changed: `lib/ecto_shorts/dynamics/adapters/postgres.ex`

Command: `mix test --seed 0 --trace`

Expected outcome: 9 doctests, 884 tests, 0 failures. No new warnings.
