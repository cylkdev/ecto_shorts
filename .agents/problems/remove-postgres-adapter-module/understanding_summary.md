# Understanding Summary

## Goal (one sentence)
Simplify the QueryBuilder API by deleting `EctoShorts.QueryBuilder.Adapters.Postgres` (and its `filters/0`), removing the `EctoShorts.QueryBuilder.Adapter` behaviour, and inlining the Postgres dispatch where it is used.

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
- `EctoShorts.QueryBuilder` delegates to `opts[:query_builder]` (defaulting to `EctoShorts.QueryBuilder.Adapters.Postgres`) for `build_query/6`.
- `EctoShorts.QueryBuilder.Adapters.Postgres` is the only adapter and dispatches:
  - `:join` → `EctoShorts.QueryBuilder.Stages.Joins.build/5`
  - `:select` / `:select_merge` → `EctoShorts.QueryBuilder.Stages.Selects.build/5`
  - everything else → `EctoShorts.QueryBuilder.Stages.Filters.build/6`
 - The effective DB “adapter boundary” is in `lib/ecto_shorts/query_builder/dynamics.ex` (when `opts[:repo]` is provided it enforces `Ecto.Adapters.Postgres`).

## Acceptance Criteria
- The module `EctoShorts.QueryBuilder.Adapters.Postgres` is removed from `lib/`.
- Default query building continues to work exactly as before for the supported filters (`:join`, `:select`, `:select_merge`, and the Filters stage).
- `mix format`, `mix compile`, and `mix test` pass.

## Confirmed Decisions (from you)
1. Remove `EctoShorts.QueryBuilder.Adapters.Postgres.filters/0` (module is truly removed).
2. Remove the QueryBuilder adapter behaviour boundary; DB “adapter boundary” lives at expression compilation in `Dynamics`.
3. No deprecation wrapper; simplify the public API.
