# Remove QueryBuilder Adapter Layer

## Status
Accepted

## Context
`EctoShorts.QueryBuilder` currently ships an adapter behaviour and a Postgres adapter module (`EctoShorts.QueryBuilder.Adapters.Postgres`).

In practice, the library only ships a Postgres implementation, and database-specific behavior is already constrained elsewhere (notably in `EctoShorts.QueryBuilder.Dynamics`, which enforces `Ecto.Adapters.Postgres` when `opts[:repo]` is provided).

The adapter layer increases API surface area (extra modules/behaviours) without meaningful value for this library’s current scope.

## Decision
- Remove `EctoShorts.QueryBuilder.Adapter`.
- Remove `EctoShorts.QueryBuilder.Adapters.Postgres` (including `filters/0`).
- Inline the Postgres dispatch logic directly into `EctoShorts.QueryBuilder`.

## Consequences
### Easier
- Smaller public API surface.
- Fewer modules/behaviours to maintain.
- Clearer architecture: DB “adapter boundary” remains at expression compilation in `Dynamics`.

### Harder / Tradeoffs
- Removal of `EctoShorts.QueryBuilder.Adapter` may break consumers that implemented the behaviour.
