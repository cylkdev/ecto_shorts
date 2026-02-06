# Task Tree — remove-postgres-adapter-module

## Problem
Remove `EctoShorts.QueryBuilder.Adapters.Postgres` and simplify the QueryBuilder API by removing the adapter behaviour boundary (it now lives at expression compilation in `Dynamics`).

## Task Group 01 — Remove QueryBuilder adapter layer (selected)

### Task 01.1 — ADR for public API change (required)
- [ ] Draft ADR proposing removal of `EctoShorts.QueryBuilder.Adapter` and `EctoShorts.QueryBuilder.Adapters.Postgres`.
- [ ] Get explicit approval for the ADR + Task Group 01 plan before changing `lib/` code.

### Task 01.2 — Inline dispatch into `EctoShorts.QueryBuilder`
- [ ] Update `EctoShorts.QueryBuilder.build_query/6` to contain the Postgres dispatch directly (no adapter module).

### Task 01.3 — Remove adapter modules/files
- [ ] Delete `lib/ecto_shorts/query_builder/adapters/postgres.ex`.
- [ ] Delete `lib/ecto_shorts/query_builder/adapter.ex`.
- [ ] Remove now-unused `lib/ecto_shorts/query_builder/adapters/` directory if empty.

### Task 01.4 — Tests + validation
- [ ] Add tests covering:
  - [ ] default dispatch still works for a representative filter (e.g. `:limit`)
- [ ] Run `mix format`, `mix compile`, `mix test`.

## Task Group 02 — Documentation & migration notes (optional follow-up)

### Task 02.1 — Docs
- [ ] Update `CHANGELOG.md` (and any README/docs if present) to note removal of `:query_builder` and adapter modules.
- [ ] Ensure ExDoc module grouping does not reference removed/non-existent modules.

## Selected Next Task Group
**Task Group 01**.
