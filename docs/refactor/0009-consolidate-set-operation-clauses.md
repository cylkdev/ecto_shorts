# Consolidate Duplicate Set-Operation Clauses in Filter

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

Six private function clauses in `EctoShorts.CommonFilters.Filter` (`apply_expr/6` for `:except`, `:except_all`, `:intersect`, `:intersect_all`, `:union`, `:union_all`) were structurally identical. Each clause called `to_query/3` on the value, then forwarded the result to the corresponding `Ecto.Query` macro. The only difference between clauses was the atom name and the macro called. This duplication meant that any change to the shared pattern (for example, adding error handling around `to_query`) would require updating six places instead of one.

After the refactor, the shared logic lives in a single guarded `apply_expr/6` clause, and a compile-time `for` comprehension generates the six one-liner dispatch clauses. The file shrank by ~15 lines and the shared pattern has a single point of change.

To verify behaviour is preserved, run `mix test --seed 0 --trace` from the repository root. All 791 tests and 9 doctests must pass.

## Progress

- [x] (2026-03-01 05:30Z) Identified the duplicate code smell in `lib/ecto_shorts/common_filters/filter.ex` lines 140–168. Six `apply_expr/6` clauses for set operations follow the identical pattern.
- [x] (2026-03-01 05:31Z) Replaced six clauses with one guarded clause + compile-time generated `apply_set_operation/3` helper. Added `@set_operations` module attribute. `mix format` clean, 791 tests + 9 doctests pass.
- [x] (2026-03-01 05:32Z) Full quality checks: `mix credo --strict` passes (pre-existing warnings only), `mix dialyzer` passes (1 pre-existing error), `mix test` - 0 failures.
- [x] (2026-03-01 05:33Z) Wrote this RefactorPlan artifact to `docs/refactor/0009-consolidate-set-operation-clauses.md`.

## Surprises & Discoveries

- Observation: The `Ecto.Query` set-operation functions (`except/2`, `union/2`, etc.) are macros, not functions. This means they cannot be called via `apply/3` or `Kernel.apply/3`. Compile-time code generation with `for` + `unquote` is the correct approach to avoid duplicating the macro calls while still letting each macro expand at compile time.
  Evidence: Attempting `apply(Query, op, [query, expr])` would fail because macros are not available at runtime.

## Decision Log

- Decision: Use a `for` comprehension at module scope to generate `apply_set_operation/3` clauses rather than a single runtime dispatch.
  Rationale: `Ecto.Query.except/2` and friends are macros that must be expanded at compile time. A runtime dispatch via `apply/3` would not work. The `for` comprehension generates one function clause per operation, each calling the correct macro. This matches the project's existing pattern of compile-time code generation (seen in `windows.ex`, `select.ex`, etc.).
  Date/Author: 2026-03-01 / Cascade

- Decision: Place `@set_operations` after `@custom_filters` to keep module attributes grouped logically.
  Rationale: `@set_operations` is used solely by `apply_expr` and `apply_set_operation`, both private functions in the same section of the file.
  Date/Author: 2026-03-01 / Cascade

## Outcomes & Retrospective

The refactor replaced six identical 3-line function clauses with one 4-line guarded clause plus a 5-line compile-time generator block. Net reduction is ~15 lines. More importantly, the shared `to_query` call now has a single point of change, so future modifications (such as adding error handling) only need to be made once.

All 791 tests and 9 doctests pass. No new credo warnings, no new dialyzer errors. The 1 pre-existing dialyzer error in `dynamics.ex` is unrelated.

No remaining smells in this area. The other `apply_expr` clauses (`:lock`, `:last`, `:limit`, etc.) each have distinct logic and do not exhibit the same duplication pattern.

## Context and Orientation

`EctoShorts.CommonFilters.Filter` is the default query builder module used by `EctoShorts.CommonFilters` for WHERE clauses, set operations, and query-level operations. It lives at `lib/ecto_shorts/common_filters/filter.ex`. The module's `build/6` function is the public entry point (marked `@doc false` since it is called only by `CommonFilters`). Internally, `build/6` delegates to `apply_expr/6` for query-level operations and `apply_where_expr/3` for WHERE conditions.

The six set operations (`:except`, `:except_all`, `:intersect`, `:intersect_all`, `:union`, `:union_all`) all follow the same pattern: convert the user-provided value into an `Ecto.Query` via `to_query/3`, then call the corresponding `Ecto.Query` macro to combine it with the base query. Before this refactor, each had its own dedicated `apply_expr/6` clause.

## Behaviour Boundary (Must Remain Unchanged)

The observable behaviour of `EctoShorts.CommonFilters.convert_params_to_filter/3` must remain unchanged. Specifically:

- Passing `%{except: value}`, `%{except_all: value}`, `%{intersect: value}`, `%{intersect_all: value}`, `%{union: value}`, or `%{union_all: value}` produces the same `Ecto.Query` struct as before.
- The `value` can be an `Ecto.Query` struct (passed through) or a filter params map (converted via `CommonFilters.convert_params_to_filter/3`).
- All 791 existing tests and 9 doctests continue to pass with seed 0.

## Code Smell Identified

**Duplicate Code** (from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`). Duplicate code means two or more code fragments that are structurally identical or nearly identical, differing only in small details. It makes maintenance harder because every change must be replicated in all copies, and readers must verify that the copies are truly identical.

In this case, the six `apply_expr/6` clauses at lines 140–168 of `lib/ecto_shorts/common_filters/filter.ex` were identical in structure. Each clause: (1) received the same six parameters, (2) called `to_query(schema_source, value, opts)`, and (3) called the corresponding `Ecto.Query` macro. The only variation was the atom in the second parameter and the macro name.

## Refactoring Technique Selected

**Extract Function** (from `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`). Extract Function means pulling a repeated or complex piece of logic into its own named function to eliminate duplication and improve clarity. The extracted function captures the shared pattern, and the original call sites are replaced with calls to the new function.

In this refactor, the shared `to_query` + macro dispatch logic was extracted into a single `apply_expr/6` clause guarded by `when op in @set_operations`, which delegates to `apply_set_operation/3`. The six one-liner `apply_set_operation/3` clauses are generated at compile time via a `for` comprehension, because the `Ecto.Query` set-operation functions are macros that must be expanded at compile time.

## Plan of Work

In `lib/ecto_shorts/common_filters/filter.ex`:

1. Add `@set_operations [:except, :except_all, :intersect, :intersect_all, :union, :union_all]` as a module attribute after `@custom_filters`.

2. Replace the six `apply_expr/6` clauses (`:except`, `:except_all`, `:intersect`, `:intersect_all`, `:union`, `:union_all`) with a single guarded clause that calls `apply_set_operation/3`.

3. Add a `for` comprehension that generates six `apply_set_operation/3` clauses, one per operation.

## Concrete Steps

All commands are run from the repository root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

    mix format lib/ecto_shorts/common_filters/filter.ex
    mix test --seed 0 --trace

Expected: 791 tests, 9 doctests, 0 failures.

    mix credo --strict

Expected: passes with pre-existing warnings only (no new issues).

    mix dialyzer

Expected: passes with 1 pre-existing error in `dynamics.ex`.

## Validation and Acceptance

Run `mix test --seed 0 --trace` from the repository root. Expect 791 tests and 9 doctests to pass with 0 failures. The set-operation tests in `test/ecto_shorts/common_filters/core_test.exs` exercise `:union`, `:union_all`, `:except`, `:intersect`, and their `_all` variants. These tests must produce identical SQL before and after the refactor.

## Idempotence and Recovery

The refactor is a single atomic edit to one file. If the edit is applied multiple times, the result is the same. To roll back, restore the six individual `apply_expr/6` clauses and remove the `@set_operations` attribute and the `for` comprehension block.

## Artifacts and Notes

Before (6 clauses, 30 lines):

    defp apply_expr(schema_source, :except, query, _binding_selector, value, opts) do
      expr = to_query(schema_source, value, opts)
      Query.except(query, ^expr)
    end
    # ... repeated 5 more times for :except_all, :intersect, :intersect_all, :union, :union_all

After (~15 lines):

    @set_operations [:except, :except_all, :intersect, :intersect_all, :union, :union_all]

    defp apply_expr(schema_source, op, query, _binding_selector, value, opts)
         when op in @set_operations do
      expr = to_query(schema_source, value, opts)
      apply_set_operation(op, query, expr)
    end

    for op <- [:except, :except_all, :intersect, :intersect_all, :union, :union_all] do
      defp apply_set_operation(unquote(op), query, expr) do
        Query.unquote(op)(query, ^expr)
      end
    end

## Interfaces and Dependencies

In `lib/ecto_shorts/common_filters/filter.ex`, the public interface is unchanged:

    @doc false
    def build(schema_source, filter, query, binding_selector, params, opts)

The new private functions are internal only:

    defp apply_set_operation(op, query, expr)

No other modules are affected.

## Milestones

### Milestone 1 - Replace duplicate clauses

Scope: Replace the six `apply_expr/6` set-operation clauses with the consolidated version. Add `@set_operations` module attribute. Run `mix format` and `mix test --seed 0 --trace`.

Files changed: `lib/ecto_shorts/common_filters/filter.ex`

Acceptance: 791 tests + 9 doctests pass with 0 failures.

### Milestone 2 - Full quality checks

Scope: Run `mix credo --strict`, `mix dialyzer`, and `mix test` to confirm no regressions.

Acceptance: All three pass. No new warnings or errors.

### Milestone 3 - Write RefactorPlan artifact

Scope: Write this document to `docs/refactor/0009-consolidate-set-operation-clauses.md`.

Acceptance: File exists and is self-contained.
