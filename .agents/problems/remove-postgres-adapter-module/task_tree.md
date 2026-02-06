# Task Tree — remove-postgres-adapter-module (extended)

## Problem
Simplify and reorganize the query-filtering internals:
- `EctoShorts.CommonFilters` should focus on param normalization + reduction.
- A top-level `EctoShorts.QueryBuilder` should own step dispatch (`build_query/6`) and contain the stage modules (`Joins`, `Selects`, `Filters`).
- `BindingHelpers` should have a clearer home.
- `dynamics/**` should be reorganized into a connected expression-compilation tree.

## Task Group 01 — Remove QueryBuilder adapter layer (completed)

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

## Task Group 02 — Remove `EctoShorts.QueryBuilder` module (completed)

### Notes
- This task group is complete, but its prior “docs” checklist items are now obsolete given the direction to reintroduce `EctoShorts.QueryBuilder` (TG03).

## Task Group 03 — Reintroduce top-level `EctoShorts.QueryBuilder` and move stages under it (proposed)

### Task 03.1 — Introduce `EctoShorts.QueryBuilder`
- [ ] Draft ADR-0004 and get explicit approval (reintroduce `EctoShorts.QueryBuilder` dispatch).
- [ ] Update ADR-0003 to match this structure and get explicit approval.
- [ ] Create `EctoShorts.QueryBuilder` with `build_query/6`.
- [ ] Update `EctoShorts.CommonFilters` to call `EctoShorts.QueryBuilder.build_query/6` instead of dispatching to stage modules directly.

### Task 03.2 — Relocate + rename stage modules
- [ ] Move `EctoShorts.QueryBuilder.Stages.Joins` → `EctoShorts.QueryBuilder.Joins`
- [ ] Move `EctoShorts.QueryBuilder.Stages.Filters` → `EctoShorts.QueryBuilder.Filters`
- [ ] Move `EctoShorts.QueryBuilder.Stages.Selects` → `EctoShorts.QueryBuilder.Selects`

### Task 03.3 — Update `EctoShorts.QueryBuilder` implementation
- [ ] Update `EctoShorts.QueryBuilder.build_query/6` to call the renamed stage modules.

### Task 03.4 — Validation
- [ ] `mix format`
- [ ] `mix compile`
- [ ] `mix test test/ecto_shorts/common_filters_test.exs`

## Task Group 04 — Move `BindingHelpers` (proposed)

### Task 04.1 — Relocate + rename
- [ ] Move/rename `EctoShorts.QueryBuilder.BindingHelpers` → `EctoShorts.QueryBuilder.Bindings` (keep under QueryBuilder domain).

### Task 04.2 — Update references
- [ ] Update stages + dynamic-expression tooling to reference the new module.

### Task 04.3 — Validation
- [ ] `mix format`
- [ ] `mix compile`

## Task Group 05 — Reorganize `QueryBuilder.Dynamics` internals (Compiler + Postgres) (completed)

### Task 05.1 — Reorganize within the `Dynamics` namespace
- [ ] Move `ParamPreprocessor` under `EctoShorts.QueryBuilder.Dynamics`.
- [ ] Rename `Dynamics.Expression` to `Dynamics.Compiler` and merge ClauseAdapter into it.
- [ ] Remove the old `ClauseEmitter` behaviour API (keep emitter validation by exported function only).
- [ ] Rename `Dynamics.Expressions.Postgres` to `Dynamics.Postgres` (remove plural `Expressions`) and move files accordingly.

### Task 05.3 — Update references + tests
- [ ] Update internal references and tests for the renamed modules.

### Task 05.4 — Validation
- [ ] `mix format`
- [ ] `mix compile`
- [ ] `mix test test/ecto_shorts/common_filters_test.exs`
- [ ] `mix test test/ecto_shorts/query_builder/dynamics/repo_adapter_opts_test.exs` (or updated path after move)

## Task Group 06 — Remove `kind` + emitter from Dynamics compiler API (completed)

### Task 06.1 — ADR for compiler API simplification (required)
- [ ] Draft ADR proposing:
  - removing `kind` from compiler callbacks and from clause specs (`ClauseSpec`),
  - removing the dedicated emitter module and making clause emission fixed to `dynamic_field_expr/3`.
- [ ] Get explicit approval for the ADR + Task Group 06 plan before changing `lib/` code.

### Task 06.2 — Simplify compiler contract + clause specs
- [ ] Remove `kind` from `EctoShorts.QueryBuilder.Dynamics.Compiler` public callbacks.
- [ ] Remove `:kind` from `EctoShorts.QueryBuilder.Dynamics.Compiler.ClauseSpec`.
- [ ] Merge clause compilation (`__after_compile__/2`) into `EctoShorts.QueryBuilder.Dynamics.Compiler` and delete `lib/ecto_shorts/query_builder/dynamics/compiler/clause_builder.ex`.
- [ ] Update clause generation to be fixed to `dynamic_field_expr/3` and delete `lib/ecto_shorts/query_builder/dynamics/compiler/emitters/dynamic_field_expr.ex`.

### Task 06.3 — Update Postgres specs + tests
- [ ] Update `EctoShorts.QueryBuilder.Dynamics.Postgres` + `Dynamics.Postgres.Specs.*` to the simplified compiler callbacks.
- [ ] Update all impacted tests and remove any remaining assumptions around `kind` and emitters.

### Task 06.4 — Validation
- [ ] `mix format`
- [ ] `mix compile`
- [ ] Run focused tests relevant to Dynamics compiler/specs.

## Task Group 07 — Remove `@after_compile` and define `Compiled` modules explicitly (completed)

### Task 07.1 — ADR for removing compile callback (required)
- [ ] Draft ADR proposing to remove `@after_compile`/`Module.create` and define `Compiled` modules explicitly with `defmodule`.
- [ ] Get explicit approval for the ADR + Task Group 07 plan before changing `lib/` code.

### Task 07.2 — Define compiled modules explicitly
- [ ] Remove `@after_compile`/`__after_compile__/2` and `Module.create/3`.
- [ ] Define `Dynamics.Postgres.Compiled.{Common,Scalar,Array}` explicitly in `postgres.ex` via `defmodule`.
- [ ] Emit `dynamic_field_expr/3` clauses via one explicit macro call per compiled module (no emitter, no kind).

### Task 07.3 — Validation
- [ ] `mix format`
- [ ] `mix compile`
- [ ] Run focused tests relevant to Dynamics compiler/specs.

## Task Group 08 — Predictable `<Caller>.Compiled` naming (completed)

### Task 08.1 — ADR (required)
- [ ] Draft ADR-0008 to define the `<Caller>.Compiled` naming rule and choose the specs source approach.
- [ ] Get explicit approval before implementation.

### Task 08.2 — Implement compiler + adopt pattern
- [ ] Update `Dynamics.Compiler.__using__/1` to define `defmodule <Caller>.Compiled` and emit clauses into it.
- [ ] Update `EctoShorts.QueryBuilder.Dynamics` so it:
  - uses `EctoShorts.QueryBuilder.Dynamics.Postgres.operators/0` to detect adapter operator keys
  - delegates predicate building to `EctoShorts.QueryBuilder.Dynamics.Postgres.build_dynamic_expression/4`
  - keeps the existing boolean-group reduction and query_fields warning behavior
- [ ] Update `EctoShorts.QueryBuilder.Dynamics.Postgres` so it exports only `operators/0` and `build_dynamic_expression/4`, and remove `Postgres.Compiled.*` ownership while Postgres expression modules use the new `<Caller>.Compiled` pattern.

### Task 08.3 — Validation
- [ ] `mix format`
- [ ] `mix compile`
- [ ] Run focused tests relevant to Dynamics compiler/specs.

## Task Group 09 — Cleanup + docs (optional)
- [ ] Remove now-empty `lib/ecto_shorts/query_builder/` directories once no longer used.
- [ ] Update ExDoc module grouping if needed to match the new structure.

## Selected Next Task Group
**Task Group 08**.
