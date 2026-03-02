# Split ArrayExpr.Specs.Core into domain-specific modules

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

`ArrayExpr.Specs.Core` was a 440-line module containing 6 distinct spec groups (list semantics, alias operators, nil handling, lower/upper case transforms, like/ilike pattern matching, and base comparison operators). Splitting the text-operation domains into their own modules makes each file focused on one concern and easier to navigate, while mirroring the pattern already established in `ScalarExpr.Specs` where aggregate, arithmetic, date-time, and quantifier specs each have their own file.

The exact runtime behaviour must remain unchanged. The `EctoShorts.Compiler` dispatch chain (`||` across compiled sub-modules) preserves clause-matching semantics as long as more-specific modules appear earlier in the `specs:` list.

Verification: `mix test --seed 0 --trace` must pass all tests with 0 failures.

## Progress

- [x] (2026-03-02) Identified the 6 spec groups in Core and their cross-delegation dependencies.
- [x] (2026-03-02) Created `Specs.LowerUpper` with extracted `lower_upper_specs/4` plus 4 interceptor clauses for `{:not, {:==/:!=, {:lower/:upper, val}}}`.
- [x] (2026-03-02) Created `Specs.LikeIlike` with extracted `like_ilike_specs/4`.
- [x] (2026-03-02) Removed extracted functions from Core and updated `clause_specs/4`.
- [x] (2026-03-02) Updated `array_expr.ex` specs list: LowerUpper, LikeIlike, Core, Aggregate.
- [x] (2026-03-02) Updated test aliases in `array_expr_specs_test.exs`.
- [x] (2026-03-02) Validated with `mix test --seed 0` and random seed. 9 doctests, 903 tests, 0 failures.

## Surprises & Discoveries

- Observation: The Compiler creates isolated compiled sub-modules for each entry in the `specs:` list. Unqualified `apply_dynamic_expr` calls inside a compiled sub-module resolve only within that sub-module, not through the top-level dispatch chain.
  Evidence: After initial extraction, 2 tests failed because Core's `list_semantic_specs` delegated `{:not, {:==, {:lower, "elixir"}}}` to `{:!=, {:lower, "elixir"}}`, which hit Core's own `base_op_specs` catch-all instead of reaching LowerUpper's `{:!=, {:lower, val}}` clause.

- Observation: The fix was to add 4 interceptor clauses to LowerUpper for `{:not, {:==/:!=, {:lower/:upper, val}}}` patterns. Since LowerUpper dispatches before Core, these clauses match first and delegate to targets within LowerUpper itself, bypassing Core's catch-all entirely.

## Decision Log

- Decision: Extract `lower_upper_specs` and `like_ilike_specs` into separate modules; keep `list_semantic_specs`, `alias_op_specs`, `nil_specs`, and `base_op_specs` together in Core.
  Rationale: LikeIlike has no cross-module delegation dependencies. LowerUpper is self-contained for its own delegations but has inbound dependencies from Core's `list_semantic_specs`. Adding 4 interceptor clauses to LowerUpper resolves this cleanly without duplicating fragment logic.
  Date/Author: 2026-03-02 / Cascade

- Decision: Place LowerUpper and LikeIlike before Core in the `specs:` dispatch order.
  Rationale: LowerUpper has more-specific head patterns (e.g., `{:==, {:lower, val}}`) that overlap with Core's catch-all patterns (e.g., `{:==, val}`). The `||` dispatch chain tries modules in list order, so more-specific modules must come first.
  Date/Author: 2026-03-02 / Cascade

## Outcomes & Retrospective

The refactor is complete. Core went from 440 lines to 299 lines. Two new focused modules were created: LowerUpper (130 lines) and LikeIlike (81 lines). All 903 tests pass with both fixed and random seeds, confirming behaviour preservation.

Key lesson: the Compiler's compiled sub-module isolation means cross-module delegation is impossible via unqualified `apply_dynamic_expr` calls. Any future split must account for inbound delegation dependencies by adding interceptor clauses to the extracted module.

## Context and Orientation

The EctoShorts Compiler (`lib/ecto_shorts/compiler.ex`) accepts a `specs:` list of modules that implement the `ClauseSpecProvider` behaviour. Each specs module returns `ClauseSpec` structs that get compiled into function clauses inside isolated sub-modules. A dispatch chain using `||` tries each sub-module in list order, falling through on `nil`.

Key files before refactoring:

- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr.ex` - parent module with `use EctoShorts.Compiler, specs: [Core, Aggregate]`
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/core.ex` - 440-line module with 6 spec groups
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/aggregate.ex` - already-extracted aggregate specs
- `test/ecto_shorts/compiler/array_expr_specs_test.exs` - unit tests for spec groups

A "compiled sub-module" is a module generated at compile time by the Compiler (e.g., `ArrayExpr.Compiled.Core`). Each entry in the `specs:` list becomes one compiled sub-module with its own `apply_dynamic_expr/3` function and a catch-all clause returning `nil`.

An "interceptor clause" is a clause added to a module specifically to match a pattern before another module's catch-all can misroute it.

## Behaviour Boundary (Must Remain Unchanged)

- `EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.apply_dynamic_expr/3` returns the same `Ecto.Query.DynamicExpr` for every input combination as before the refactor.
- All existing tests in the suite pass without modification to assertions.
- The public API surface of `ArrayExpr` is unchanged (only `apply_dynamic_expr/3`).

## Code Smell Identified

`Large Module` from `.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`. The `ArrayExpr.Specs.Core` module contained 440 lines covering 6 unrelated spec domains (list semantics, alias operators, nil handling, case transforms, pattern matching, base comparisons). A large module is one that has grown to contain multiple distinct responsibilities, making it harder to navigate and reason about which section handles which concern.

## Refactoring Technique Selected

`Extract Function` generalized to `Extract Module` from `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`. The technique moves a cohesive group of functions into a new module with the same interface (here, `ClauseSpecProvider`), reducing the size and responsibility count of the original module while preserving all observable behaviour.

## Plan of Work

1. Create `Specs.LowerUpper` module containing `lower_upper_specs/4` plus 4 interceptor clauses for patterns that Core's `list_semantic_specs` would otherwise misroute.
2. Create `Specs.LikeIlike` module containing `like_ilike_specs/4`.
3. Remove the extracted functions from Core and update its `clause_specs/4`.
4. Update the `specs:` list in `array_expr.ex` with correct dispatch ordering.
5. Update test aliases.

## Concrete Steps

All commands run from the repository root: `/Users/kurthogarth/Documents/GitHub/ecto_shorts`

1. Create `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/lower_upper.ex`.
2. Create `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/like_ilike.ex`.
3. Edit `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/core.ex`: remove `lower_upper_specs/4` and `like_ilike_specs/4`, update `clause_specs/4`.
4. Edit `lib/ecto_shorts/dynamics/adapters/postgres/array_expr.ex`: update specs list.
5. Edit `test/ecto_shorts/compiler/array_expr_specs_test.exs`: add aliases, update calls.
6. Run validation:

        mix test --seed 0 --trace

   Expected: 9 doctests, 903 tests, 0 failures.

## Validation and Acceptance

Run `mix test --seed 0 --trace` from the repository root. All 903 tests and 9 doctests must pass with 0 failures. Additionally run `mix test` with a random seed to confirm no ordering-dependent regressions.

Actual result:

    9 doctests, 903 tests, 0 failures (seed 0)
    9 doctests, 903 tests, 0 failures (seed 573387)

## Idempotence and Recovery

The refactor is purely additive (two new files) and subtractive (functions removed from Core). Running the steps again on a clean checkout produces the same result. To revert, restore `core.ex` from version control and delete the two new files.

## Artifacts and Notes

Files created:
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/lower_upper.ex` (130 lines)
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/like_ilike.ex` (81 lines)

Files modified:
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs/core.ex` (440 -> 299 lines)
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr.ex` (2 -> 4 specs in list)
- `test/ecto_shorts/compiler/array_expr_specs_test.exs` (alias updates)

## Interfaces and Dependencies

In `lib/ecto_shorts/dynamics/adapters/postgres/array_expr.ex`, preserve:

    def apply_dynamic_expr(binding_selector, key, expr) :: Ecto.Query.DynamicExpr.t() | nil

All three spec modules (`Core`, `LowerUpper`, `LikeIlike`) implement:

    @behaviour EctoShorts.Compiler.ClauseSpecProvider
    @callback clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) :: [ClauseSpec.t()]

## Milestones

### Milestone 1 - Extract LowerUpper and LikeIlike

Goal: Move `lower_upper_specs/4` and `like_ilike_specs/4` into dedicated modules. Add interceptor clauses to LowerUpper for cross-module delegation safety. Update Core, parent module, and tests.

Files added: `specs/lower_upper.ex`, `specs/like_ilike.ex`
Files changed: `specs/core.ex`, `array_expr.ex`, `array_expr_specs_test.exs`

Command: `mix test --seed 0 --trace`
Expected: 9 doctests, 903 tests, 0 failures.
Actual: 9 doctests, 903 tests, 0 failures. Confirmed with random seed 573387.
