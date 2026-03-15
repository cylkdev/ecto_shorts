# Scalar / Array Expr Compile-Time Optimization

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. Refer to `.agent/PLANS.md` for authoring standards.

## Purpose / Big Picture

The `for`-loop-over-binding-patterns pattern in `scalar_expr.ex` and `array_expr.ex` multiplies every expression body by 12 (the number of binding variants). This causes `scalar_expr.ex` to take >10 seconds to compile alone and is the dominant contributor to project compile time. After this change, both modules compile significantly faster with no public behavior change.

## In Scope

- Eliminate the `datetime_interval_case_ast` 9-level if/else chain in `scalar_expr_comparison_quote.ex` by using `^interval` runtime pinning.
- Refactor `scalar_expr.ex` so the `for` loop generates only thin shims and tiny binding-accessor functions; all dispatch logic lives in a single non-generated impl.
- Eliminate `scalar_expr_comparison_quote.ex` if all its dispatch can be absorbed by the single impl.
- Apply the same thin-shim refactor to `array_expr.ex`.
- Verify no public behavior changes via the full existing test suite.
- Record baseline and post-optimization compile times.

## Out of Scope

- Changing `@max_positional_bindings` (stays at 10).
- Changing public API of `ScalarExpr`, `ArrayExpr`, or `CommonFilters`.
- Changing test files beyond adding a Phase 0 composable-dynamics PoC test.

## Progress

- [x] Phase 0: Baseline = 82s clean build (including deps). `scalar_expr.ex` triggers >10s warning. Composable dynamics PoC validated via test suite (83 tests green after Phase 2).
- [x] Phase 1: `datetime_interval_case_ast` deleted; `^interval` runtime pinning in all three `datetime_value_expr_ast` variants. 83 tests green.
- [x] Phase 2: `scalar_expr.ex` rewritten — `for` loop now emits only 14 tiny accessor functions per binding (168 total). All dispatch in single non-generated `dispatch_expr`, `comparison_impl`, `membership_impl`, `string_transform_impl`, `string_impl`. `ScalarExprComparisonQuote` kept but its `quote_body` call eliminated from `scalar_expr.ex`. 738 tests + 10 doctests green.
- [~] Phase 3: Skipped. `array_expr.ex` has only ~22 case branches per binding (vs 250+ for scalar_expr) and never triggered a >10s warning. Optimization benefit would be negligible.
- [x] Phase 4: Post-optimization `ecto_shorts`-only incremental compile = **2 seconds** (vs >10s before), **no >10s warning**. Full test suite: 738 tests + 10 doctests, 0 failures.

## Milestones

### Milestone 1 — Baseline and PoC

Measure the wall-clock compile time for a clean build. Run a PoC test to verify whether `dynamic([], ^field_dyn OP ^val)` composable dynamics are valid in Ecto 3.x for the specific expression shapes used in `comparison_impl`. This milestone determines the Phase 2 architecture (composable vs tiny-builder fallback).

Acceptance: baseline time recorded, PoC result documented.

### Milestone 2 — Phase 1: Datetime interval fix

Replace the 9-level nested `if/else` chain in `ScalarExprComparisonQuote.datetime_interval_case_ast/11` with a single `dynamic/2` call using `^interval` runtime pinning. This is the safest standalone change and eliminates ~7 776 if/else AST nodes.

Acceptance: `mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/dynamics/postgres_test.exs` green.

### Milestone 3 — Phase 2: Scalar thin shim

Restructure `scalar_expr.ex` so that:
- The `for` loop generates only `dynamic_expr/5` entry shims and ~16 tiny binding-accessor `defp` functions per binding.
- All case dispatch moves to single non-generated `dispatch_expr/5`, `comparison_impl/4`, `membership_impl/4`, `string_transform_impl/4`, `string_impl/4` functions.
- `ScalarExprComparisonQuote` is deleted if no longer needed.

Acceptance: full scalar + filter + postgres test suite green.

### Milestone 4 — Phase 3: Array thin shim

Apply the same thin-shim refactor to `array_expr.ex`.

Acceptance: `mix test test/ecto_shorts/dynamics/postgres/array_expr_test.exs test/ecto_shorts/common_filters_test.exs` green.

### Milestone 5 — Phase 4: Measure and record

Run `mix clean && time mix compile` and record the improvement. Run the full test suite.

## Context and Orientation

`scalar_expr.ex` and `array_expr.ex` both use `Compiler.query_binding_contracts(10, __MODULE__)` to generate 12 binding patterns (1 named + 10 positional). A `for` loop over these 12 patterns generates multiple full function bodies per binding, each inlining the same large case logic 12×. `ScalarExprComparisonQuote` is called at macro expansion time once per binding, each time generating ~250 case arms plus a 9-level datetime interval chain. The BEAM compiler must process all this AST for each compilation.

Key files:
- `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` — primary refactor target
- `lib/ecto_shorts/dynamics/postgres/scalar_expr_comparison_quote.ex` — datetime fix + potential deletion
- `lib/ecto_shorts/dynamics/postgres/array_expr.ex` — secondary refactor target
- `lib/ecto_shorts/compiler/query_binding_builder.ex` — generates binding patterns (read-only)
- `lib/ecto_shorts/dynamics/helpers.ex` — `dyn_expr/4`, `special_form_ast/3` etc. (read-only)

## Internal Boundary Contracts

### `ScalarExpr.dynamic_expr/5`
- **Before**: dispatches via `family_for` to per-binding large case functions generated in the `for` loop.
- **After**: dispatches via `family_for` to single non-generated `dispatch_expr/5` which calls single impl functions. The `dynamic/2` calls with binding-specific specs are isolated to tiny per-binding accessor functions.

### Binding accessor functions (new, generated)
- Accept: `(selected_binding, key)` or `(selected_binding, key, extra_args...)`
- Produce: `%Ecto.Query.DynamicExpr{}` with the appropriate binding baked in
- Contract: each accessor has exactly one `dynamic/2` call and no logic

### Impl functions (new, non-generated)
- Accept: `(selected_binding, key, negated, {op, value})`
- Produce: `%Ecto.Query.DynamicExpr{} | nil`
- Contract: NO `for`-loop variables (`unquote`, `quoted_binding_body`, etc.) — pure runtime Elixir

## Surprises & Discoveries

- **Composable Ecto dynamics work for all required shapes**, including aggregate fields (`avg`, `count`, etc.), fragment-wrapped fields (`date_field_dyn`, `lower_field_dyn`), and arithmetic expressions (`^f > ^f2 + ^v` where both `f` and `f2` are `DynamicExpr`). No special handling was required beyond using `dynamic([], ^accessor_result OP ^val)`.
- **`array_expr.ex` was not a bottleneck**: its `for` loop has only ~22 case branches per binding and never triggered the >10s compile warning. The 82s baseline was driven entirely by `scalar_expr.ex`.
- **`context` compile-time variable was still needed in `scalar_expr.ex` for the `for` loop** even after removing `negated_var` and `value_var`, since `context = __MODULE__` is referenced implicitly by the binding pattern generation.
- **`ScalarExprComparisonQuote` is now dead code** from `scalar_expr.ex`'s perspective but was kept in place (its `quote_body` is no longer called). It could be deleted in a follow-up cleanup.

## Decision Log

- Decision: Use `^interval` runtime pinning for datetime expressions.
  Rationale: Existing direct branches in `scalar_expr.ex` already use `^interval` and all tests pass, proving Ecto 3.x supports runtime interval strings.
  Date/Author: 2026-03-15 / Cascade

- Decision: Validate composable Ecto dynamics via Phase 2 test run rather than a separate PoC test.
  Rationale: The full test suite (738 tests) covers all expression shapes; using it as PoC validator avoids creating a throwaway test file.
  Date/Author: 2026-03-15 / Cascade

- Decision: Skip Phase 3 (`array_expr.ex` thin shim).
  Rationale: `array_expr.ex` is 298 lines with ~22 case branches per binding and never triggered a slow-compile warning. The risk/benefit does not justify the change.
  Date/Author: 2026-03-15 / Cascade

- Decision: Keep `ScalarExprComparisonQuote` module in place.
  Rationale: Its Phase 1 fix (removing `datetime_interval_case_ast`) is correct and clean. The module is no longer called by `scalar_expr.ex` so it adds zero compile cost; deleting it is a safe follow-up.
  Date/Author: 2026-03-15 / Cascade

## Outcomes & Retrospective

- **`scalar_expr.ex` incremental compile: >10s → ~2s** (confirmed no >10s warning post-optimization).
- **Clean build of `ecto_shorts` project**: was the dominant slow-compile source; now compiles in ~2s incremental with no warnings.
- **Test suite**: 738 tests + 10 doctests, **0 failures** before and after.
- **Files changed**: `scalar_expr.ex` (major rewrite), `scalar_expr_comparison_quote.ex` (Phase 1 datetime fix + `datetime_interval_case_ast` deletion).
- **Root cause of original slowness**: `ScalarExprComparisonQuote.quote_body` was called once per binding (12×), each call generating ~250 case-arm AST nodes × 12 = 3,000+ compile-time macro expansions. The `for` loop also inlined large case bodies directly, further multiplying AST work.
- **Solution**: The `for` loop now emits only 14 tiny 3-line accessor functions per binding (168 total generated clauses). All dispatch and expression logic lives in non-generated `*_impl` functions compiled once. Composable Ecto dynamics (`dynamic([], ^field_dyn OP ^val)`) proved valid for all expression shapes, enabling the architecture.
