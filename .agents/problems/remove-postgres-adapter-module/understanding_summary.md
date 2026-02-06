# Understanding Summary

## Goal (one sentence)
Simplify the query-building API by removing the `EctoShorts.QueryBuilder` module entirely and moving its dispatch functionality into the only call site (`EctoShorts.CommonFilters`).

## Hard Constraints
- Follow the repo’s Review Driven Development loop: **Understand → Decompose → Plan → Plan Review (approval) → Act → Implementation Review → Iterate**.
- Write review artifacts under `.agents/problems/remove-postgres-adapter-module/` before asking for review/approval.
- No public API / context-boundary changes without an ADR + explicit approval.
- Elixir style rules: `@moduledoc` on modules; `@doc` on public functions; return `{:ok, _}` / `{:error, _}` for new public APIs (avoid exceptions for control flow).

## Non-goals
- Add support for non-Postgres Ecto adapters (this change is refactoring only).
- Change query semantics or stage behavior (`Filters`, `Joins`, `Selects` remain the source of truth).

## Inputs / Outputs
- **Inputs:** current filter key (e.g. `:join`, `:select`, `:select_merge`, or other filter keys), args, opts.
- **Outputs:** an `Ecto.Query.t()` with the filter applied.

## Current State (as observed)
- `EctoShorts.QueryBuilder` currently dispatches:
  - `:join` → `EctoShorts.QueryBuilder.Stages.Joins.build/5`
  - `:select` / `:select_merge` → `EctoShorts.QueryBuilder.Stages.Selects.build/5`
  - everything else → `EctoShorts.QueryBuilder.Stages.Filters.build/6`
 - The effective DB “adapter boundary” is in `lib/ecto_shorts/query_builder/dynamics.ex` (when `opts[:repo]` is provided it enforces `Ecto.Adapters.Postgres`).
 - The only runtime call site of `EctoShorts.QueryBuilder.build_query/6` is `EctoShorts.CommonFilters` (`apply_query_builder/6`).

## Acceptance Criteria
- The module `EctoShorts.QueryBuilder` is removed from `lib/`.
- Default query building continues to work exactly as before for the supported filters (`:join`, `:select`, `:select_merge`, and the Filters stage).
- `mix format`, `mix compile`, and `mix test` pass.

## Confirmed Decisions (from you)
1. Remove `EctoShorts.QueryBuilder.Adapters.Postgres.filters/0` (module is truly removed).
2. Remove the QueryBuilder adapter behaviour boundary; DB “adapter boundary” lives at expression compilation in `Dynamics`.
3. No deprecation wrapper; simplify the public API.
4. Remove the `EctoShorts.QueryBuilder` module; move its dispatch into the call site.
