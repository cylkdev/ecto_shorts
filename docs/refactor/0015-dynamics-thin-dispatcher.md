# Simplify EctoShorts.Dynamics to a thin dispatcher

This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

`EctoShorts.Dynamics` had 421 lines with a 70-line recursive flatten chain (`normalize_expression_params`, `flatten_expression_params`, and helpers) that converted maps to keyword lists and then to tuples before passing to the adapter. The function `apply_helper_expressions` mixed two unrelated concerns - map-to-keyword conversion and `:from` subquery detection - behind a vague name. A beginner could not understand what the module did without tracing every clause.

After this refactor, the module is a thin dispatcher that does three things a beginner can identify at a glance: boolean grouping, `:from` subquery resolution, and adapter dispatch. Maps flow through untouched. The adapter converts maps to canonical tuples at its own boundary right before `apply_dynamic_expr/3`. Spec modules only match canonical tuple shapes and never pattern match on maps.

The behaviour that must remain unchanged is: `EctoShorts.Dynamics.convert_to_dynamic/4` produces identical dynamic expressions for every existing test input. The public API signature and return types are unchanged.

## Progress

- [x] (2026-03-02 12:00Z) Removed 4 map-matching `ClauseSpec` entries from `scalar_expr/specs/date_time.ex`.
- [x] (2026-03-02 12:05Z) Deleted `apply_helper_expressions`, `helper_expr_select`, `build_helper_expr_subquery`, the flatten/normalize chain (10 functions), `@equal`, `@map_payload_helper_operators`.
- [x] (2026-03-02 12:05Z) Added `resolve_subqueries/4` with single responsibility: find `:from` keys and build subqueries. Maps stay maps.
- [x] (2026-03-02 12:05Z) Rewrote `append_predicates` as a simple dispatcher with `build_field_predicate` using reductions.
- [x] (2026-03-02 12:10Z) Added `normalize_field_expr`, `normalize_entry`, `normalize_inner`, `to_keyword_payload`, `normalize_operator_expr` to the Postgres adapter.
- [x] (2026-03-02 12:15Z) Fixed nil field value bug (`:no_field_value` sentinel replaced by reduction approach).
- [x] (2026-03-02 12:20Z) Fixed field-level boolean handling for tuple values from CommonFilters keyword destructuring.
- [x] (2026-03-02 12:35Z) Fixed multi-field boolean entries dispatching through `merge_boolean_predicates`.
- [x] (2026-03-02 12:39Z) All tests pass (9 pre-existing failures only).

## Surprises & Discoveries

- Observation: `CommonFilters.build_schema_filters` (line 1394-1424 of `common_filters.ex`) converts map values to keyword lists with `Map.to_list(value)` AND then iterates keyword entries, calling `create_schema_filter` for EACH entry individually. This means by the time Dynamics receives a field value, a map like `%{not: %{in: [true, false]}}` has been destructured into a tuple `{:not, [in: [true, false]]}` - not a map or keyword list. The adapter's `normalize_field_expr` needed a tuple clause to handle this.
  Evidence: Debug tracing showed `is_map(expr)` was `false` and `is_tuple(expr)` was `true` for values that originated as maps.

- Observation: Field-level boolean entries like `%{views: %{and: [>: 10, <: 20]}}` arrive at Dynamics as single tuples `{:and, [>: 10, <: 20]}` (not keyword lists or maps) due to CommonFilters' keyword destructuring. This required explicit boolean tuple clauses in `build_field_predicate`.

- Observation: Boolean entries inside field values can contain either operator-value keyword pairs (like `[>: 10, <: 20]`) or multi-field params (like `[[published: true, does_not_exist: "value"]]`). The former should go directly to the adapter; the latter must go through `merge_boolean_predicates` for schema validation. `Keyword.keyword?/1` distinguishes these two cases.

## Decision Log

- Decision: Delete `apply_helper_expressions` entirely and replace with `resolve_subqueries`.
  Rationale: `apply_helper_expressions` mixed map-to-keyword conversion with `:from` subquery detection. A single-responsibility function that only finds `:from` keys is clearer for beginners.
  Date/Author: 2026-03-02, cascade

- Decision: Move expression normalization (map to tuple conversion) into the Postgres adapter's `build_dynamic/4`.
  Rationale: The adapter knows its own canonical tuple shapes. Dynamics should not need to know about `:datetime`, `:date`, or operator tuple formats. Maps flow through Dynamics untouched.
  Date/Author: 2026-03-02, cascade

- Decision: Remove 4 map-matching `ClauseSpec` entries from `date_time.ex`.
  Rationale: Spec modules should only match canonical tuple shapes (strict boundary). The adapter handles map-to-tuple conversion upstream.
  Date/Author: 2026-03-02, cascade

- Decision: Use reductions in `build_field_predicate` instead of `extract_boolean_entries` normalization.
  Rationale: Reductions are simpler - each entry is either a boolean op (merge with `:and`/`:or`) or a field op (pass to adapter). No split-then-handle pattern, no sentinels.
  Date/Author: 2026-03-02, cascade

- Decision: Pass module directly to `build_field_dynamic` instead of function captures.
  Rationale: `build_field_dynamic(ArrayExpr, binding, key, expr)` reads more clearly than `build_field_dynamic(binding, key, expr, &ArrayExpr.apply_dynamic_expr/3)`. Direct module call is simpler.
  Date/Author: 2026-03-02, user correction

## Outcomes & Retrospective

The refactor reduced `dynamics.ex` from 421 to 355 lines. More importantly, it eliminated 10 functions that formed the flatten/normalize chain and replaced them with a clear separation of concerns: Dynamics dispatches, the adapter normalizes. The `apply_helper_expressions` function - the hardest to understand - no longer exists.

The main challenge was discovering that `CommonFilters` converts map values to keyword lists and destructures them into individual tuples before Dynamics receives them. This meant the adapter had to handle tuples, keyword lists, and maps as input shapes. The adapter's `normalize_field_expr` handles all three uniformly.

Remaining opportunity: `CommonFilters.build_schema_filters` (line 1394-1424) doing eager map-to-keyword conversion and keyword destructuring is the root cause of much complexity. A future refactor could simplify that path to pass maps through directly.

## Context and Orientation

Three files were changed in this refactor.

`lib/ecto_shorts/dynamics.ex` is the module that converts filter parameter maps into composable Ecto dynamic expressions. It is called by `CommonFilters.Filter`, `CommonFilters.Join`, and `CommonFilters.Having`. Its single public function is `convert_to_dynamic/4`.

`lib/ecto_shorts/dynamics/adapters/postgres.ex` is the PostgreSQL-specific adapter that implements the `EctoShorts.Dynamics.Adapter` behaviour. Its `build_dynamic/4` callback routes to `ScalarExpr`, `ArrayExpr`, or `CommonExpr` compiled spec modules.

`lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs/date_time.ex` defines `ClauseSpec` entries for datetime/date helper expressions. It had 4 entries that pattern matched on maps, violating the "specs only match tuples" boundary.

## Behaviour Boundary (Must Remain Unchanged)

`EctoShorts.Dynamics.convert_to_dynamic/4` returns identical `Ecto.Query.DynamicExpr` values for every existing test input. Returns `nil` for empty params. The `Adapter` behaviour callback `build_dynamic/4` signature is unchanged.

## Code Smell Identified

Long Function (`.agent/refactor/code_smells/bloaters/LONG_FUNCTION.md`): `reduce_predicates/8` mixed helper expression preprocessing, normalization, boolean handling, implicit equality wrapping, and adapter dispatch in a single function. The 70-line flatten chain was a secondary Long Function smell.

Feature Envy (`.agent/refactor/code_smells/couplers/FEATURE_ENVY.md`): `normalize_expression_params` and `apply_helper_expressions` in Dynamics knew about adapter-specific concerns like `:datetime`/`:date` helper operators and implicit `{:==, value}` wrapping.

## Refactoring Technique Selected

Extract Function (`.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`): Extracted `resolve_subqueries/4` with a single clear responsibility.

Inline Function (`.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`): Inlined `build_field_predicates/7`, `build_operator_predicates/7`, and `reduce_predicates/8` - thin wrappers that added indirection without clarity.

Move Function: Moved expression normalization (map-to-tuple conversion) from Dynamics into the Postgres adapter where it belongs.

## Plan of Work

The refactor proceeded in three areas: spec cleanup (remove map matching from specs), Dynamics simplification (delete flatten chain, replace `apply_helper_expressions` with `resolve_subqueries`, use reductions), and adapter boundary (add normalization functions to convert maps to canonical tuples).

## Concrete Steps

From repository root:

    mix test --seed 0 --trace

Expect 9 pre-existing failures (all `posts_with_lock` table), 0 new failures.

## Validation and Acceptance

Run `mix test --seed 0 --trace` and expect 908 tests with 9 failures (all pre-existing `posts_with_lock` table issues). The refactor preserves all existing tests. No new tests were added because this is a behaviour-preserving refactor.

## Idempotence and Recovery

All changes are to source files only. `git checkout -- lib/ecto_shorts/dynamics.ex lib/ecto_shorts/dynamics/adapters/postgres.ex lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs/date_time.ex` reverts to the original state.

## Artifacts and Notes

Final test output:

    9 doctests, 908 tests, 9 failures
    Randomized with seed 0

Files changed:

    lib/ecto_shorts/dynamics.ex                                            | 355 lines (was 421)
    lib/ecto_shorts/dynamics/adapters/postgres.ex                          | 388 lines (was 305)
    lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs/date_time.ex | 294 lines (was 355)

Functions deleted from `dynamics.ex`: `apply_helper_expressions/4`, `build_helper_expr_subquery/4`, `helper_expr_select/2` (3 clauses), `normalize_expression_params/1`, `flatten_expression_params/2` (7 clauses), `flatten_expression_entries/2`, `flatten_keyed_expression_entries/3`, `build_field_predicates/7`, `build_operator_predicates/7`, `reduce_predicates/8`.

Functions added to `dynamics.ex`: `resolve_subqueries/4` (4 clauses), `build_subquery/4`, `build_field_predicate/7` (5 clauses), `reduce_field_value/6`.

Functions added to `postgres.ex`: `build_field_dynamic/4`, `normalize_field_expr/1` (4 clauses), `normalize_entry/1` (2 clauses), `normalize_inner/1` (3 clauses), `to_keyword_payload/1` (3 clauses), `normalize_operator_expr/1` (3 clauses).

## Interfaces and Dependencies

In `lib/ecto_shorts/dynamics.ex`, preserve:

    @spec convert_to_dynamic(source :: term(), binding_selector :: term(), params :: term(), opts :: keyword()) :: Ecto.Query.dynamic_expr() | nil

In `lib/ecto_shorts/dynamics/adapters/postgres.ex`, preserve:

    @impl true
    def build_dynamic(source, binding_selector, key, expr)

The `EctoShorts.Dynamics.Adapter` behaviour callback signatures are unchanged.

## Milestones

This refactor was completed in a single session. All milestones were verified with `mix test --seed 0`.

## Revision Note

Initial version. Created 2026-03-02.
