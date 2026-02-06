# Understanding Summary

## Goal (one sentence)
Reorganize the query-filtering internals so `EctoShorts.CommonFilters` focuses on param normalization, while a top-level `EctoShorts.QueryBuilder` owns step dispatch and contains stage modules; then relocate bindings and reorganize expression compilation (`dynamics/**`) into a connected tree (including simplifying the Dynamics compiler API).

## Hard Constraints
- Follow the repo’s Review Driven Development loop: **Understand → Decompose → Plan → Plan Review (approval) → Act → Implementation Review → Iterate**.
- Write review artifacts under `.agents/problems/remove-postgres-adapter-module/` before asking for review/approval.
- No public API / context-boundary changes without an ADR + explicit approval.
- Elixir style rules: `@moduledoc` on modules; `@doc` on public functions; return `{:ok, _}` / `{:error, _}` for new public APIs (avoid exceptions for control flow).

## Non-goals
- Add support for non-Postgres Ecto adapters.
- Change query semantics, binding semantics (`:as`/`:at`), or operator behavior (refactor-only).
- Introduce new dependencies.

## Inputs / Outputs
- **Inputs:** current filter key (e.g. `:join`, `:select`, `:select_merge`, or other filter keys), args, opts.
- **Outputs:** an `Ecto.Query.t()` with the filter applied.

## Current State (as observed)
- `EctoShorts.QueryBuilder` was removed; dispatch now lives in `EctoShorts.CommonFilters.apply_query_builder/6`.
- Stage modules still live under `EctoShorts.QueryBuilder.Stages.*` and are invoked by `EctoShorts.CommonFilters`.
- `EctoShorts.QueryBuilder.BindingHelpers` is used by stage modules (and dynamic-expression tooling) but is no longer conceptually a “QueryBuilder” concern.
- `EctoShorts.QueryBuilder.Dynamics` and `lib/ecto_shorts/query_builder/dynamics/**` implement dynamic-expression compilation, but their namespace/location is now disconnected from the entrypoint domain (`CommonFilters`).

## Acceptance Criteria
- Stage modules are relocated under `EctoShorts.CommonFilters` (no longer `EctoShorts.QueryBuilder.Stages.*`).
- `EctoShorts.QueryBuilder.BindingHelpers` is moved to a clearer namespace (proposal below) and all internal call sites are updated.
- The `dynamics/**` modules are reorganized into a cohesive namespace aligned to the filter domain (proposal below) and internal call sites/tests are updated.
- The Dynamics clause compiler no longer exposes `kind` in its public API (no `kind` arguments and no `:kind` on clause specs).
- The dedicated emitter module is removed; clause generation is fixed to `dynamic_field_expr/3` and owned by the library.
- Focused validation passes:
  - `mix format`
  - `mix compile`
  - `mix test test/ecto_shorts/common_filters_test.exs`
  - `mix test test/ecto_shorts/query_builder/dynamics/repo_adapter_opts_test.exs` (or updated path after reorg)

## Confirmed Decisions (from you)
1. Remove `EctoShorts.QueryBuilder.Adapters.Postgres.filters/0` (module is truly removed).
2. Remove the QueryBuilder adapter behaviour boundary; DB “adapter boundary” lives at expression compilation in `Dynamics`.
3. No deprecation wrapper; simplify the public API.
4. Remove the `EctoShorts.QueryBuilder` module; move its dispatch into the call site.
5. Remove `kind` from the Dynamics compiler API (arguments + clause spec data).
6. Remove `EctoShorts.QueryBuilder.Dynamics.Compiler.Emitters.DynamicFieldExpr`; the library should own the fixed clause emission for `dynamic_field_expr/3`.
7. Keep `EctoShorts.QueryBuilder.Dynamics.Compiler` as the **contract + macro** entrypoint; keep compile-time module generation logic out of it so the compiler stays “dumb”.
8. `use EctoShorts.QueryBuilder.Dynamics.Compiler` should create a predictable nested module named `<Caller>.Compiled` (by appending `.Compiled` to the caller’s module name).

## Proposed placements (to review)
- Dispatch + stages: `EctoShorts.QueryBuilder.build_query/6` and `EctoShorts.QueryBuilder.{Filters, Joins, Selects}`
- Binding helper: keep under QueryBuilder domain; rename to `EctoShorts.QueryBuilder.Bindings`
- Dynamics: keep `EctoShorts.QueryBuilder.Dynamics` as the runtime entrypoint, but reorganize its subtree for clarity:
  - `Dynamics.Expressions.Postgres` → `Dynamics.Postgres` (remove plural “Expressions”)
  - `Dynamics.Expression` → `Dynamics.Compiler` and merge ClauseAdapter into it
