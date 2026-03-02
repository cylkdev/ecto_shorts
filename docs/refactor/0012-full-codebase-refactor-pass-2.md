# Full Codebase Refactor Pass 2: Deduplicate Specs, Simplify Dispatch, Extract Bulk

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

This refactor addresses the remaining code smells identified after refactor 0001 (Extract Focused Submodules). The largest wins come from deduplicating compile-time AST code across the two expression specs files, simplifying a 16-arm case dispatch in CommonFilters, and extracting bulk operations from the still-oversized Actions module. After the refactor, the codebase has fewer copy-pasted code blocks, a data-driven dispatch pattern replacing a long case statement, and one more focused submodule in the Actions namespace.

To verify behaviour is preserved, run `mix test --seed 0` from the repository root. All 898 tests and 9 doctests must pass.

## Progress

- [x] (2026-03-02 04:45Z) Milestone 1: Created `ExprHelpers` module. Replaced 12 inline alias-to-canonical mapping copies and 4 duplicate aggregate case AST helpers across `scalar_expr/specs.ex` and `array_expr/specs.ex`. 898 tests pass.
- [x] (2026-03-02 04:50Z) Milestone 2: Replaced 16-arm case dispatch in `common_filters.ex` `build_query/6` with `@query_builder_modules` map-based dispatch. 898 tests pass.
- [x] (2026-03-02 04:55Z) Milestone 3: Extracted `Actions.Bulk` from `actions.ex`. Bulk operations (`insert_all`, `update_all`, `delete_all`) now delegate to `actions/bulk.ex`. Removed unused `CommonParams` alias. 898 tests pass.
- [x] (2026-03-02 05:00Z) Milestone 4: Skipped. `normalize_insert_entry` in `common_params.ex` is already well-structured via pattern matching.
- [x] (2026-03-02 05:05Z) Milestone 5: Consolidated `build_field_predicates/7` and `build_operator_predicates/7` in `dynamics.ex` into shared `reduce_predicates/8` + `build_and_merge/8`. 898 tests pass.
- [x] (2026-03-02 05:10Z) Milestone 6: Skipped. `join.ex` option extraction is inside `Compiler.define_clauses` macro block. Risk outweighs benefit.
- [x] (2026-03-02 05:12Z) Milestone 7: Skipped. `utils.ex` inlining requires `mix.exs` change (restricted) and test migration for a 47-line module.
- [x] (2026-03-02 05:15Z) Milestone 8: Final validation. `mix test`: 9 doctests, 898 tests, 0 failures. `mix credo --strict`: no new warnings. `mix dialyzer`: no new warnings.

## Surprises & Discoveries

- Observation: The `==/2` Credo warnings in `expr_helpers.ex` are from quoted AST code that generates Ecto dynamic expressions. These use `==` intentionally because they become Ecto query expressions, not Elixir equality checks. The same warnings existed in the original specs files.
  Evidence: `mix credo --strict` shows the same warning count before and after.

- Observation: The `normalize_insert_entry` dispatch in `common_params.ex` has 6 clauses but each handles a genuinely different input shape with distinct logic. This is well-structured pattern matching, not a code smell.
  Evidence: Clause 1 redirects `{changeset, params}` to `{struct, params}`. Clauses 2-6 each have different normalization and build steps.

- Observation: The `join.ex` option extraction pattern (`qualifier`, `prefix`, `as`, `hints`) appears inside `Compiler.define_clauses` which generates function clauses at compile time per binding pattern. Extracting a helper within this macro context risks subtle compile-time behavior changes for only 4 lines of savings per clause.
  Evidence: The `define_clauses` block generates clauses for every binding pattern (named, positional, shortcut).

## Decision Log

- Decision: Use `Map.get(@query_builder_modules, filter_op)` instead of a case dispatch for `build_query/6`.
  Rationale: Follows the same pattern established in refactor 0001 for `apply_query_builder`. The map lookup is O(1) and makes adding new query builders a single-line map entry change instead of a new case arm.
  Date/Author: 2026-03-02 / Cascade

- Decision: Keep `:subquery` as a separate `build_query/6` clause instead of including it in `@query_builder_modules`.
  Rationale: The `:subquery` handler has unique pre-filtering logic (it applies filters to the query before calling `SubQuery.build`) and a fallback clause for invalid params. This was the same decision made in refactor 0001.
  Date/Author: 2026-03-02 / Cascade

- Decision: Skip milestones 4, 6, and 7 after investigation.
  Rationale: M4's target was already well-structured. M6's target was inside a compile-time macro block with high risk. M7 required modifying a restricted file (`mix.exs`). In each case the risk or effort outweighed the benefit.
  Date/Author: 2026-03-02 / Cascade

- Decision: Create `ExprHelpers` as a `@moduledoc false` module with public functions rather than extracting helpers as macros.
  Rationale: The shared functions (`alias_to_canonical_map_ast/1`, `aggregate_dynamic_case_ast/4`, `not_aggregate_dynamic_case_ast/4`) return quoted AST. They are called at compile time from the specs modules. Public functions work correctly because the specs modules call them during their own compilation. Using macros would add unnecessary complexity.
  Date/Author: 2026-03-02 / Cascade

## Outcomes & Retrospective

The refactor achieved its primary goals. The three highest-severity duplicate code smells were eliminated, a long case dispatch was replaced with a data-driven map, and the Actions module gained another focused submodule.

Line count summary:

    Before:
      scalar_expr/specs.ex   1758
      array_expr/specs.ex     935
      common_filters.ex      1606
      actions.ex             1974
      dynamics.ex             497
      Total                 17379

    After:
      scalar_expr/specs.ex   1587  (-10%)
      array_expr/specs.ex     795  (-15%)
      common_filters.ex      1581  (-2%)
      actions.ex             1955  (-1%)
      dynamics.ex             482  (-3%)
      expr_helpers.ex         123  (new)
      actions/bulk.ex          47  (new)
      Total                 17192  (-187 net)

Follow-up opportunities:

- `actions.ex` at 1955 lines is mostly documentation (~1200 lines). The code portion (~700 lines) could be further split if CRUD compound operations (`find_and_*`) are extracted.
- The `scalar_expr/specs.ex` `all_dynamic_case_ast`, `not_all_dynamic_case_ast`, `any_dynamic_case_ast`, `not_any_dynamic_case_ast`, `date_time_comparison_case_ast`, and `not_date_time_comparison_case_ast` private helpers follow the same pattern as the now-extracted aggregate helpers. They could be generalized into `ExprHelpers` with a higher-order function that accepts an AST builder. This was not done in this pass because each helper has a slightly different comparison expression structure.
- `utils.ex` could still be inlined if the user approves the `mix.exs` change.

## Context and Orientation

EctoShorts is an Elixir library providing a data-driven API for Ecto. This refactor builds on refactor 0001 which extracted submodules from the three largest files. The key files changed in this pass are:

- `lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs.ex` - Compile-time spec generation for scalar field dynamic expressions. Had 12 copies of the alias-to-canonical operator mapping and 2 copies of aggregate case AST helpers.
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs.ex` - Same pattern for array field expressions. Had 4 copies of alias mapping and 2 copies of aggregate case helpers.
- `lib/ecto_shorts/dynamics/adapters/postgres/expr_helpers.ex` - New shared module for deduplicated AST helpers.
- `lib/ecto_shorts/common_filters.ex` - Main filter-to-query conversion. The `build_query/6` private function had a 16-arm case dispatch.
- `lib/ecto_shorts/actions.ex` - Public CRUD, bulk, multi, batch, and transaction API. Bulk operations extracted.
- `lib/ecto_shorts/actions/bulk.ex` - New module for bulk operation internals.
- `lib/ecto_shorts/dynamics.ex` - Dynamic expression builder. Two near-duplicate predicate builder functions consolidated.

## Behaviour Boundary (Must Remain Unchanged)

- All public functions in `EctoShorts.Actions` retain their signatures and return shapes.
- `EctoShorts.CommonFilters.convert_params_to_filter/3` returns the same `Ecto.Query.t()`.
- All dynamic expression adapters produce the same SQL output.
- All 898 tests and 9 doctests pass with `mix test --seed 0`.

## Code Smell Identified

1. **Duplicate Code** (`.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`): The `case op do :gt -> :> ...` alias-to-canonical mapping block was copy-pasted 12 times across `scalar_expr/specs.ex` and `array_expr/specs.ex`. The `aggregate_dynamic_case_ast/4` and `not_aggregate_dynamic_case_ast/4` private functions were identically defined in both files.

2. **Long Function** (`.agent/refactor/code_smells/bloaters/LONG_FUNCTION.md`): `build_query/6` in `common_filters.ex` was a 16-arm case statement where every arm called `Module.build(binding_source, filter_op, query, binding_selector, params, opts)` with only the module varying.

3. **Large Module** (`.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`): `actions.ex` still contained CRUD, bulk, multi, batch, and transaction operations after refactor 0001.

4. **Duplicate Code**: `build_field_predicates/7` and `build_operator_predicates/7` in `dynamics.ex` shared the same `apply_helper_expressions` + `normalize_expression_params` + reduce + warning pattern.

## Refactoring Technique Selected

1. **Extract Function** (`.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`): Used to create `alias_to_canonical_map_ast/1`, `aggregate_dynamic_case_ast/4`, and `not_aggregate_dynamic_case_ast/4` in `ExprHelpers`.

2. **Replace Function with Module** (`.agent/refactor/techniques/composing_functions/REPLACE_FUNCTION_WITH_MODULE.md`): Used to extract `Actions.Bulk`.

3. **Extract Function**: Used to consolidate `build_field_predicates/7` and `build_operator_predicates/7` into `reduce_predicates/8` + `build_and_merge/8`.

## Plan of Work

See the Progress section. All milestones are complete.

## Concrete Steps

From the repository root:

    mix format
    mix test --seed 0 --trace
    mix credo --strict
    mix dialyzer

Expected: 898 tests, 9 doctests, 0 failures. Credo passes (pre-existing warnings only). Dialyzer shows pre-existing warnings only.

## Validation and Acceptance

Run `mix test --seed 0` and observe 9 doctests, 898 tests, 0 failures. The refactor preserves all existing tests. No new tests were needed because no behaviour changed.

## Idempotence and Recovery

All changes are additive extractions followed by call-site updates. Running the steps again produces the same result. If any step fails partway, `git checkout -- lib/` restores the original state.

## Artifacts and Notes

Test output after final validation:

    Finished in 2.3 seconds (2.3s async, 0.00s sync)
    9 doctests, 898 tests, 0 failures
    Randomized with seed 0

## Interfaces and Dependencies

In `lib/ecto_shorts/actions.ex`, all public `def` functions unchanged. Internal delegation now additionally goes through:

    EctoShorts.Actions.Bulk.insert_all/3
    EctoShorts.Actions.Bulk.update_all/4
    EctoShorts.Actions.Bulk.delete_all/3

In `lib/ecto_shorts/dynamics/adapters/postgres/expr_helpers.ex`, three new `@doc false` public functions:

    EctoShorts.Dynamics.Adapters.Postgres.ExprHelpers.alias_to_canonical_map_ast/1
    EctoShorts.Dynamics.Adapters.Postgres.ExprHelpers.aggregate_dynamic_case_ast/4
    EctoShorts.Dynamics.Adapters.Postgres.ExprHelpers.not_aggregate_dynamic_case_ast/4

In `lib/ecto_shorts/dynamics.ex`, two new private functions:

    reduce_predicates/8
    build_and_merge/8

## Milestones

See the Progress section. All 8 milestones are complete (4 executed, 3 skipped with rationale, 1 final validation).

## Revision Note

Initial completion - all milestones executed or skipped with rationale, validated, and retrospective written. 2026-03-02.
