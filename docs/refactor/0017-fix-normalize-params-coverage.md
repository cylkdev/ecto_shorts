# Fix normalize_params to cover full flatten_expression_params behavior

This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

Reference: `.agent/REFACTOR_PLANS.md` at the repository root.

## Purpose / Big Picture

The `normalize_params/2` function in the Postgres adapter was introduced in refactor 0016 to replace the old `flatten_expression_params/1`, `do_flatten/2`, and `flatten_operator_expr/1` functions. The new function used a simpler cond-based dispatch but had three bugs that caused 473 test failures. This refactor fixes those bugs so `normalize_params/2` produces identical output to the old functions it replaced, restoring the full test suite to green.

After this refactor, the Postgres adapter has a single, readable normalization function that converts nested filter expressions into flat lists of `{operator, value}` tuples for dispatch to `ScalarExpr`, `ArrayExpr`, and `CommonExpr`.

## Progress

- [x] (2025-07-17) Identified three bugs in `normalize_params/2` and `reduce_expr/4` via test output analysis.
- [x] (2025-07-17) Fixed Bug 1: moved `normalize_params` call into per-branch dispatch in `build_dynamic/4`.
- [x] (2025-07-17) Fixed Bug 2: added map/keyword/catch-all branches to `normalize_params({key, value}, acc)` and fixed infinite recursion in `normalize_params(term, acc)`.
- [x] (2025-07-17) Fixed Bug 3: corrected `reduce_expr/4` nil accumulator check (`is_nil(dyn_left)` instead of `is_nil(dyn_right)`).
- [x] (2025-07-17) Fixed Bug 4: corrected `@datetime_operators` branch to preserve value as a single unit instead of splitting map entries apart.
- [x] (2025-07-17) Deleted commented-out old code.
- [x] (2025-07-17) Full test suite passes: 9 doctests, 884 tests, 0 failures.

## Surprises & Discoveries

- Observation: The `normalize_params(term, acc)` clause for `Keyword.keyword?(term)` called itself with identical arguments, causing infinite recursion and 60-second test timeouts for any expression containing keyword lists.
  Evidence: Stack traces showed `normalize_params/2` calling itself at line 350 repeatedly until ExUnit timeout.

- Observation: The `reduce_expr/4` function checked `is_nil(dyn_right)` instead of `is_nil(dyn_left)`. Since `dyn_right` is guaranteed non-nil (the `nil` case is handled by the preceding `case` branch), this check never triggered. On the first iteration, `dyn_left` was `nil` (the accumulator initial value), causing `Query.dynamic([], ^nil and ^dyn_right)` which injected a literal `nil` parameter into every generated query.
  Evidence: Every failing test showed `$1 AND (...)` with `[nil, ...]` in the parameter list.

- Observation: The `@datetime_operators` branch used `Enum.reduce(value, acc, ...)` which split map entries apart. The old `do_flatten` for boundary operators preserved the value as a single unit: `[{:datetime, Map.to_list(v)} | acc]` for maps and `[{:datetime, v} | acc]` for lists.
  Evidence: 18 datetime-related test failures with `CastError` showing the value was incorrectly decomposed.

## Decision Log

- Decision: Move `normalize_params` into per-branch calls instead of calling it once before the cond in `build_dynamic/4`.
  Rationale: `CommonExpr.compose/3` expects a single raw value (e.g. `[1, 2]` for `:ids`, `{:not, subquery}` for `:exists`), not a list of `{op, value}` tuples. Calling `normalize_params` for the `@operators` path would wrap values in `{:==, ...}` or decompose tuples. The simplest fix is to skip normalization entirely for `@operators` and wrap `expr` in `[expr]` so `reduce_expr` iterates once with the raw value. User confirmed this approach.

- Decision: Fix `reduce_expr` nil check inline rather than restructuring the function.
  Rationale: Single-character fix (`dyn_right` → `dyn_left`) that restores correct first-iteration handling. The logic is: on the first iteration the accumulator is `nil`, so return `dyn_right` directly; on subsequent iterations, combine with AND.

- Decision: Replace `Enum.reduce` in `@datetime_operators` branch with direct prepend.
  Rationale: The old `do_flatten` for boundary operators preserved the value as a single unit. `Enum.reduce` incorrectly iterated over map entries, splitting `%{add: %{...}}` into `{:add, %{...}}` as a separate entry.

## Outcomes & Retrospective

All four bugs are fixed. The full test suite passes with 0 failures and 0 new warnings. The `normalize_params/2` function now produces identical output to the old `flatten_expression_params/1` + `do_flatten/2` for all input shapes:

- Scalar values → `[{:==, value}]`
- Maps → converted to keyword list, recursed
- Keyword lists → recursed entry-by-entry
- Tuples `{op, scalar}` → `[{op, scalar}]` (no wrapping)
- Tuples `{op, map}` → map converted to list, recursed, wrapped with op
- Tuples `{op, keyword}` → keyword recursed, wrapped with op
- Datetime tuples `{:datetime/:date, map}` → `[{op, Map.to_list(map)}]`
- Datetime tuples `{:datetime/:date, list}` → `[{op, list}]`

The commented-out old code has been removed, leaving a clean single implementation.

## Context and Orientation

The file `lib/ecto_shorts/dynamics/adapters/postgres.ex` defines `EctoShorts.Dynamics.Postgres`, the PostgreSQL-specific dynamic expression adapter. It implements the `EctoShorts.Dynamic` behavior with one public callback `build_dynamic/4`.

`build_dynamic/4` routes filter expressions to one of three sub-modules:

- `CommonExpr` — handles special operators (`:ids`, `:before`, `:after`, `:start_date`, `:end_date`, `:exists`)
- `ArrayExpr` — handles array field operations
- `ScalarExpr` — handles scalar field operations

For `ArrayExpr` and `ScalarExpr`, filter expressions are first normalized by `normalize_params/2` into a flat list of `{operator, value}` tuples, then each tuple is dispatched via `reduce_expr/4` which calls `compose/3` on the sub-module.

For `CommonExpr`, the raw expression is wrapped in a single-element list `[expr]` and passed to `reduce_expr/4` directly, since `CommonExpr.compose/3` expects the original value shape.

## Behavior Boundary (Must Remain Unchanged)

`EctoShorts.Dynamics.Postgres.build_dynamic/4` returns the same `Ecto.Query.dynamic_expr()` for all input shapes as before refactor 0016. Verified by the full test suite: 9 doctests, 884 tests, 0 failures.

## Code Smell Identified

`Dead Code` from `.agent/refactor/code_smells/dispensables/DEAD_CODE.md`. The commented-out old functions (`flatten_expression_params/1`, `do_flatten/2`, `flatten_operator_expr/1`) were dead code left as a reference. They are removed now that the replacement is verified.

Additionally, three bugs in the replacement code constituted incorrect behavior from an incomplete `Inline Function` application:

1. Over-recursion into scalar values (wrapping in `{:==, ...}`)
2. Infinite recursion for keyword lists
3. Wrong nil check in accumulator

## Refactoring Technique Selected

`Inline Function` from `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`. The three old functions were consolidated into one `normalize_params/2` function. This refactor completes that consolidation by fixing the behavior gaps.

## Plan of Work

All changes are in `lib/ecto_shorts/dynamics/adapters/postgres.ex`:

1. In `build_dynamic/4`, remove the `normalized = normalize_params(expr, [])` call before the cond. For `@operators`, use `reduce_expr(CommonExpr, binding_selector, key, [expr])`. For Array/Scalar, call `normalize_params(expr, [])` inline.

2. In `normalize_params({key, value}, acc)`, split the `@datetime_operators` branch into map and non-map sub-branches that prepend directly. Add `is_map(value)` and `Keyword.keyword?(value)` branches before the catch-all. Change the catch-all from recursing into `value` to prepending `{key, value}` directly.

3. In `normalize_params(term, acc)`, fix the `Keyword.keyword?(term)` branch from `normalize_params(term, acc)` (infinite recursion) to `Enum.reduce(term, acc, fn entry, acc2 -> normalize_params(entry, acc2) end)`.

4. In `reduce_expr/4`, change `is_nil(dyn_right)` to `is_nil(dyn_left)` and swap the branch bodies.

5. Delete all commented-out old code (lines 358–441).

## Concrete Steps

All commands run from `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

    mix test --seed 0 --trace

Expected output:

    9 doctests, 884 tests, 0 failures
    Randomized with seed 0

## Validation and Acceptance

Run:

    mix test --seed 0 --trace

Accept when: 9 doctests, 884 tests, 0 failures. No new warnings.

## Idempotence and Recovery

All changes are in a single file. Running `mix test` at any point shows the current state. The changes can be reverted by restoring the file from version control.

## Artifacts and Notes

Final state of `normalize_params/2` (two clauses):

    defp normalize_params({key, value}, acc) do
      cond do
        key in @datetime_operators and is_map(value) and not is_struct(value) ->
          [{key, Map.to_list(value)} | acc]

        key in @datetime_operators ->
          [{key, value} | acc]

        is_map(value) and not is_struct(value) ->
          value
          |> Map.to_list()
          |> normalize_params([])
          |> Enum.reduce(acc, fn entry, acc2 ->
            [{key, entry} | acc2]
          end)

        Keyword.keyword?(value) ->
          value
          |> normalize_params([])
          |> Enum.reduce(acc, fn entry, acc2 ->
            [{key, entry} | acc2]
          end)

        true ->
          [{key, value} | acc]
      end
    end

    defp normalize_params(term, acc) do
      cond do
        is_map(term) and not is_struct(term) ->
          term
          |> Map.to_list()
          |> normalize_params(acc)

        Keyword.keyword?(term) ->
          Enum.reduce(term, acc, fn entry, acc2 ->
            normalize_params(entry, acc2)
          end)

        true ->
          [{:==, term} | acc]
      end
    end

## Interfaces and Dependencies

In `lib/ecto_shorts/dynamics/adapters/postgres.ex`, preserve:

    @impl true
    def build_dynamic(source, binding_selector, key, expr) :: Ecto.Query.dynamic_expr()

    @impl true
    def operators() :: [atom()]

    @impl true
    def operator?(key) :: boolean()

No new public functions introduced. Only private helpers `normalize_params/2` and `reduce_expr/4` were modified.

## Milestones

### Milestone 1: Fix all four bugs and delete old code

Goal: `normalize_params/2` produces identical output to the old functions for all input shapes. `reduce_expr/4` correctly handles the nil accumulator. `build_dynamic/4` routes `@operators` without normalization.

Files changed: `lib/ecto_shorts/dynamics/adapters/postgres.ex`

Command: `mix test --seed 0 --trace`

Expected: 9 doctests, 884 tests, 0 failures.

Outcome: Achieved. All 884 tests pass with 0 failures.
