# Task Group 01 Checklist

- [x] Add ADR record under `.agents/adr/` and get explicit approval.
- [x] Update `lib/ecto_shorts/query_builder.ex` to inline the Postgres dispatch (no separate adapter module).
- [x] Delete `lib/ecto_shorts/query_builder/adapters/postgres.ex`.
- [x] Delete `lib/ecto_shorts/query_builder/adapter.ex`.
- [x] Add a new test module covering:
  - [x] default dispatch for at least one filter (e.g. `:limit`)
- [x] Run `mix format` and ensure no changes remain.
- [x] Run `mix compile`.
- [x] Run `mix test`.

*** Add File: .agents/adr/0001-remove-query-builder-adapter-layer.md
# Remove QueryBuilder Adapter Layer

## Status
Proposed

## Context
`EctoShorts.QueryBuilder` currently supports a pluggable adapter option via `opts[:query_builder]`, defaulting to `EctoShorts.QueryBuilder.Adapters.Postgres`.

In practice, the library only ships a Postgres adapter, and database-specific behavior is already constrained elsewhere (notably in `EctoShorts.QueryBuilder.Dynamics`, which enforces `Ecto.Adapters.Postgres` when `opts[:repo]` is provided).

The adapter layer increases API surface area (extra modules/behaviours and an option) without meaningful value for this library’s current scope.

## Decision
- Remove support for `opts[:query_builder]` in `EctoShorts.QueryBuilder.build_query/6`.
- Remove `EctoShorts.QueryBuilder.Adapter`.
- Remove `EctoShorts.QueryBuilder.Adapters.Postgres` (including `filters/0`).
- Inline the Postgres dispatch logic directly into `EctoShorts.QueryBuilder`.
- If callers pass `query_builder:` in opts, raise `ArgumentError` with a migration-oriented message.

## Consequences
### Easier
- Smaller public API surface.
- Fewer modules/behaviours to maintain.
- Clearer architecture: DB “adapter boundary” remains at expression compilation in `Dynamics`.

### Harder / Tradeoffs
- Breaking change for consumers that provided custom `:query_builder` adapters.
- Removal of `EctoShorts.QueryBuilder.Adapter` may break consumers that implemented the behaviour.
