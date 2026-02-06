# Task Group 04 Plan — Keep bindings under QueryBuilder domain (rename + reorganize)

## Proposed new place
Keep binding-pattern generation under the `EctoShorts.QueryBuilder` domain, but rename/restructure it so it reads as a first-class QueryBuilder concern:

- `EctoShorts.QueryBuilder.BindingHelpers` → `EctoShorts.QueryBuilder.Bindings`
- File: `lib/ecto_shorts/query_builder/binding_helpers.ex` → `lib/ecto_shorts/query_builder/bindings.ex`

Rationale:
- Bindings are used exclusively by the QueryBuilder internals (stages + dynamic clause compilation).
- Keeping it under QueryBuilder reduces “domain leakage” into `CommonQuery` and keeps the QueryBuilder folder cohesive.

## Spec (expected behavior)
- No behavior changes; only module name/path changes.
- All macro-generated binding heads remain identical.

## Files/modules affected (bounded)
- `lib/ecto_shorts/query_builder/binding_helpers.ex` (move + module rename)
- Any modules importing/aliasing `EctoShorts.QueryBuilder.BindingHelpers` (stages, expression compiler)

## Acceptance criteria
- No references remain to `EctoShorts.QueryBuilder.BindingHelpers` (replaced by `EctoShorts.QueryBuilder.Bindings`).
- `mix format` and `mix compile` succeed.

## Checklist
- [ ] Move file to `lib/ecto_shorts/query_builder/bindings.ex` and rename module to `EctoShorts.QueryBuilder.Bindings`.
- [ ] Update all call sites (stages + expression compiler) to reference the new module.
- [ ] `mix format`
- [ ] `mix compile`
