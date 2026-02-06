# Task Group 06 Plan — Remove `kind` + emitter indirection from `Dynamics.Compiler`

## Goal (one sentence)
Simplify the Dynamics compiler contract by removing the `kind` tag from the public API and eliminating the dedicated emitter module, making clause emission fixed to `dynamic_field_expr/3`.

## Constraints / non-goals
- Do not change query semantics; this is a refactor-only change.
- Keep `EctoShorts.QueryBuilder.Dynamics` as the runtime entrypoint.
- Avoid folding in TG03/TG04 scope (QueryBuilder dispatch + bindings move).

## Spec (expected behavior)
### Clause specs
- A clause spec represents exactly one `dynamic_field_expr/3` function clause:
  - head patterns: `binding_head`, `key`, `head`
  - optional guard AST
  - body AST
- There is no `:kind` field on clause specs.

### Compiler adapter contract
- `use EctoShorts.QueryBuilder.Dynamics.Compiler` continues to register an `@after_compile` hook.
- Adapters no longer implement `emitter_module/0`.
- Adapters implement a single `clause_specs/4` function (no `kind` argument).
- The compiler generates compiled modules under `Adapter.Compiled.{Common,Scalar,Array}` to preserve the existing scalar/array/common dispatch, but:
  - `:common` / `:scalar` / `:array` are not threaded through the callback arguments,
  - there is no `:kind` field on clause specs.
  - the adapter can use the `context` argument (the compiled module) to choose the correct spec set.

### Clause emission
- The library owns the clause emission shape:
  - `Dynamics.Compiler` generates quoted `def dynamic_field_expr/3` clauses directly.
  - `lib/ecto_shorts/query_builder/dynamics/compiler/emitters/dynamic_field_expr.ex` is deleted.

## Inputs / outputs
- **Inputs:** adapter module, binding patterns derived from `BindingHelpers.query_var_and_binding_heads/2`, clause spec data.
- **Outputs:** compiled `Adapter.Compiled.*` modules containing `dynamic_field_expr/3` clauses.

## Edge cases / failure modes
- Specs missing required keys should still return a `NimbleOptions.ValidationError` from `ClauseSpec.new/1`.
- Specs with a guard should produce `def ... when ...` clauses.
- `ClauseBuilder.__after_compile__/2` should raise clear errors when an adapter is missing required callbacks.

## Files/modules to touch (bounded)
- `lib/ecto_shorts/query_builder/dynamics/compiler.ex`
- `lib/ecto_shorts/query_builder/dynamics/compiler/clause_builder.ex` (delete; move functionality into `compiler.ex`)
- `lib/ecto_shorts/query_builder/dynamics/compiler/clause_spec.ex`
- `lib/ecto_shorts/query_builder/dynamics/compiler/emitters/dynamic_field_expr.ex` (delete)
- `lib/ecto_shorts/query_builder/dynamics/postgres.ex`
- `lib/ecto_shorts/query_builder/dynamics/postgres/specs/common_exprs.ex`
- `lib/ecto_shorts/query_builder/dynamics/postgres/specs/scalar_exprs.ex`
- `lib/ecto_shorts/query_builder/dynamics/postgres/specs/array_exprs.ex`
- Tests under `test/ecto_shorts/query_builders/postgres/dynamics/specs/*`

## Tests to update
- Update ClauseSpec and ClauseBuilder tests to remove `kind` usage and emitter assumptions.
- Update any compilation/spec group tests that currently pass `kind` into `clause_specs/5`.

## Acceptance criteria
- No `:kind` field remains in `%ClauseSpec{}` or in clause spec maps built by Postgres spec modules.
- No code references remain to:
  - `emitter_module/0`
  - `EctoShorts.QueryBuilder.Dynamics.Compiler.Emitters.DynamicFieldExpr`
- `mix format` and `mix compile` succeed.
- Focused tests pass (compiler/spec tests + prior focused suite used in TG05).

## Checklist
- [ ] Draft/update ADR-0005 and get explicit approval for this task group plan.
- [ ] Update `Dynamics.Compiler` callbacks to remove `kind` + emitter indirection (single `clause_specs/4`).
- [ ] Remove `:kind` from `ClauseSpec` and update docs/tests.
- [ ] Move clause compilation functionality from `ClauseBuilder` into `Dynamics.Compiler` (keep `@after_compile` hook, emit `dynamic_field_expr/3` directly).
- [ ] Update `Dynamics.Postgres` + `Dynamics.Postgres.Specs.*` to match the new callbacks and remove `kind` from specs.
- [ ] Delete `lib/ecto_shorts/query_builder/dynamics/compiler/emitters/dynamic_field_expr.ex` and remove any now-empty dirs.
- [ ] Delete `lib/ecto_shorts/query_builder/dynamics/compiler/clause_builder.ex` (merged into `compiler.ex`) and update references.
- [ ] Update impacted tests.
- [ ] Verify via `rg` that old names (`:kind`, emitter module, emitter callbacks) are removed.
- [ ] Run `mix format`, `mix compile`, and focused `mix test`.
