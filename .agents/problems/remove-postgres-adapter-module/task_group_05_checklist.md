# Task Group 05 Checklist

- [x] Move `ParamPreprocessor` under `Dynamics` (`EctoShorts.QueryBuilder.Dynamics.ParamPreprocessor`) and update references.
- [x] Rename `Dynamics.Expression` → `Dynamics.Compiler` (compiler entrypoint).
- [x] Merge ClauseAdapter into `Dynamics.Compiler` and delete `dynamics/expression/clause_adapter.ex`.
- [x] Remove ClauseEmitter and delete `dynamics/expression/clause_emitter.ex`.
- [x] Update emitter modules to drop `@behaviour`/`@impl` and rely on exported `quoted_def/6` only.
- [x] Update `ClauseBuilder` docs to stop referencing `ClauseEmitter`.
- [x] Move/rename remaining `expression/**` helpers under `compiler/**` (`ClauseBuilder`, `ClauseSpec`, `AST`, emitter module(s)).
- [x] Rename/move `Dynamics.Expressions.Postgres.*` → `Dynamics.Postgres.*` (including file paths).
- [x] Update all internal references + tests (note: `BindingHelpers` rename is deferred to the QueryBuilder re-org task group).
- [x] Verify no references remain to removed names (via `rg` for the old modules/namespaces).
- [x] Run `mix format`.
- [x] Run `mix compile`.
- [x] Run focused tests.
