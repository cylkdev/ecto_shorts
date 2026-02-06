# Task Group 05 Plan — Reorganize `QueryBuilder.Dynamics` internals (Compiler + Postgres)

## Motivation
The current `lib/ecto_shorts/query_builder/dynamics/**` subtree mixes:
- a runtime entrypoint (`EctoShorts.QueryBuilder.Dynamics`) that builds Ecto `dynamic/2` expressions,
- a spec-driven clause compiler (`Dynamics.Expression.*` + `Dynamics.Expression.ClauseAdapter`),
- a Postgres “expression group” namespace (`Dynamics.Expressions.Postgres.*`),
- an older emitter behaviour (`ClauseEmitter`) that doesn’t add much beyond “exports `quoted_def/6`”.

We want this to read like a cohesive QueryBuilder domain:
- Keep the runtime entrypoint named `Dynamics` (it maps to Ecto’s `dynamic/2` and avoids churn).
- Replace `Expression`/`ClauseAdapter`/`ClauseEmitter` with a simpler compiler shape that’s still explicit.
- Remove plural “Expressions” from the Postgres namespace (`Dynamics.Postgres`).

## Proposed end state (module namespace + paths)

### Keep the runtime entrypoint
- Keep `EctoShorts.QueryBuilder.Dynamics` as the runtime entrypoint for building `dynamic/2` expressions from filter params.
  - Rationale: “dynamics” matches Ecto terminology (`dynamic/2`) and avoids churn for callers/tests.

### Move `ParamPreprocessor` under the `Dynamics` namespace
- Move `ParamPreprocessor` under `Dynamics` so it’s no longer a “floating” top-level helper:
  - `lib/ecto_shorts/query_builder/param_preprocessor.ex`
    → `lib/ecto_shorts/query_builder/dynamics/param_preprocessor.ex`
  - `EctoShorts.QueryBuilder.ParamPreprocessor`
    → `EctoShorts.QueryBuilder.Dynamics.ParamPreprocessor`

### Rename + simplify the clause compiler surface
Replace the current “Expression + ClauseAdapter + ClauseEmitter” shape with a single compiler entrypoint:

- `EctoShorts.QueryBuilder.Dynamics.Expression`
  → `EctoShorts.QueryBuilder.Dynamics.Compiler`
- Merge `ClauseAdapter` into the compiler module:
  - Delete `lib/ecto_shorts/query_builder/dynamics/expression/clause_adapter.ex`
  - `use EctoShorts.QueryBuilder.Dynamics.Compiler` becomes the adapter hook
  - `Dynamics.Compiler` defines callbacks + `__using__/1` that sets the behaviour + `@after_compile` hook (same as ClauseAdapter did)
- Remove `ClauseEmitter`:
  - Delete `lib/ecto_shorts/query_builder/dynamics/expression/clause_emitter.ex`
  - Emitters are validated by `function_exported?(emitter, :quoted_def, 6)` only (no `@behaviour`, no `@impl`)
  - Update `ClauseBuilder` docs to no longer reference `ClauseEmitter`

Move/rename the remaining helper modules under a consistent namespace/path:
- `Dynamics.Expression.ClauseBuilder` → `Dynamics.Compiler.ClauseBuilder`
- `Dynamics.Expression.ClauseSpec` → `Dynamics.Compiler.ClauseSpec`
- `Dynamics.Expression.Emitters.DynamicFieldExpr` → `Dynamics.Compiler.Emitters.DynamicFieldExpr`
- `Dynamics.Expression.AST` → `Dynamics.Compiler.AST`

### Rename Postgres clause spec groups to remove plural “Expressions”
- Rename the Postgres adapter namespace to read as a concrete implementation (not a bag of “expressions”):
  - `EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres`
    → `EctoShorts.QueryBuilder.Dynamics.Postgres`
  - Move files:
    - `lib/ecto_shorts/query_builder/dynamics/expressions/postgres.ex`
      → `lib/ecto_shorts/query_builder/dynamics/postgres.ex`
    - `lib/ecto_shorts/query_builder/dynamics/expressions/postgres/**`
      → `lib/ecto_shorts/query_builder/dynamics/postgres/**`
  - Rename children accordingly:
    - `Dynamics.Expressions.Postgres.{ArrayExpr,CommonExpr,ScalarExpr}`
      → `Dynamics.Postgres.{ArrayExpr,CommonExpr,ScalarExpr}`
    - `Dynamics.Expressions.Postgres.Specs.*`
      → `Dynamics.Postgres.Specs.*`

## File map (high-level)
- `lib/ecto_shorts/query_builder/param_preprocessor.ex`
  → `lib/ecto_shorts/query_builder/dynamics/param_preprocessor.ex`
- `lib/ecto_shorts/query_builder/dynamics/expression.ex`
  → `lib/ecto_shorts/query_builder/dynamics/compiler.ex`
- `lib/ecto_shorts/query_builder/dynamics/expression/clause_adapter.ex`
  → (deleted; merged into `compiler.ex`)
- `lib/ecto_shorts/query_builder/dynamics/expression/clause_emitter.ex`
  → (deleted; emitter validated by `quoted_def/6` export)
- `lib/ecto_shorts/query_builder/dynamics/expression/*`
  → `lib/ecto_shorts/query_builder/dynamics/compiler/*`
- `lib/ecto_shorts/query_builder/dynamics/expressions/postgres.ex`
  → `lib/ecto_shorts/query_builder/dynamics/postgres.ex`
- `lib/ecto_shorts/query_builder/dynamics/expressions/postgres/**`
  → `lib/ecto_shorts/query_builder/dynamics/postgres/**`

## Spec (expected behavior)
- No behavior changes:
  - `EctoShorts.QueryBuilder.Filters` and `EctoShorts.QueryBuilder.Joins` still build the same `dynamic/2` expressions.
  - Repo adapter enforcement (`Ecto.Adapters.Postgres`) remains identical.
- Only module/path names change.

## Tests to update
Update any modules/tests referencing:
- `EctoShorts.QueryBuilder.ParamPreprocessor` → `EctoShorts.QueryBuilder.Dynamics.ParamPreprocessor`
- `EctoShorts.QueryBuilder.Dynamics.Expression.*` → `EctoShorts.QueryBuilder.Dynamics.Compiler.*`
- `EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.*` → `EctoShorts.QueryBuilder.Dynamics.Postgres.*`
- `EctoShorts.QueryBuilder.Dynamics.Expression.ClauseEmitter` (remove references; module deleted)
- `EctoShorts.QueryBuilder.BindingHelpers` → `EctoShorts.QueryBuilder.Bindings` (from TG04)

## Acceptance criteria
- No references remain to the old names/paths:
  - `EctoShorts.QueryBuilder.ParamPreprocessor`
  - `EctoShorts.QueryBuilder.Dynamics.Expression.*`
  - `EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter`
  - `EctoShorts.QueryBuilder.Dynamics.Expression.ClauseEmitter`
  - `EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.*`
- `mix format` and `mix compile` succeed.
- Focused tests pass:
  - `mix test test/ecto_shorts/common_filters_test.exs`
  - `mix test test/ecto_shorts/query_builder/dynamics/repo_adapter_opts_test.exs` (or updated path after move)
 - A ripgrep check for removed names returns no hits:
   - `EctoShorts.QueryBuilder.ParamPreprocessor`
   - `EctoShorts.QueryBuilder.Dynamics.Expression.`
   - `EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres`
   - `ClauseEmitter` / `ClauseAdapter` (under the old namespace)

## Checklist
- [ ] Move/rename `ParamPreprocessor` under `Dynamics` and update references.
- [ ] Create/rename compiler entrypoint `Dynamics.Compiler` and merge ClauseAdapter into it.
- [ ] Remove ClauseEmitter (delete module; remove behaviour annotations; update docs).
- [ ] Move/rename remaining compiler helper modules under `Dynamics.Compiler.*`.
- [ ] Rename/move the Postgres adapter namespace to `Dynamics.Postgres.*`.
- [ ] Update all internal references + tests.
- [ ] `mix format`
- [ ] `mix compile`
- [ ] Run focused tests listed above.
