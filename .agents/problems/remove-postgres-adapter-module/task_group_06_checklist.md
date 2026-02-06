# Task Group 06 Checklist

- [x] ADR-0005 approved (remove `kind` + emitter indirection from `Dynamics.Compiler`).
- [x] Update `lib/ecto_shorts/query_builder/dynamics/compiler.ex` to remove `kind` + `emitter_module/0` and keep a single `clause_specs/4`.
- [x] Update `lib/ecto_shorts/query_builder/dynamics/compiler/clause_spec.ex` to remove `:kind` and adjust validation/docs/tests.
- [x] Move `@after_compile` and clause compilation from `lib/ecto_shorts/query_builder/dynamics/compiler/clause_builder.ex` into `lib/ecto_shorts/query_builder/dynamics/compiler.ex` (emit `dynamic_field_expr/3` directly).
- [x] Update `lib/ecto_shorts/query_builder/dynamics/postgres.ex` to match new callbacks and remove emitter usage.
- [x] Update `lib/ecto_shorts/query_builder/dynamics/postgres/specs/*.ex` to remove `kind` from function args and generated specs.
- [x] Delete `lib/ecto_shorts/query_builder/dynamics/compiler/emitters/dynamic_field_expr.ex` and remove any empty directories.
- [x] Delete `lib/ecto_shorts/query_builder/dynamics/compiler/clause_builder.ex` after the merge and update any remaining references.
- [x] Update tests under `test/ecto_shorts/query_builders/postgres/dynamics/specs/*` to the new API (no `kind`, no emitter module).
- [x] `rg` confirms no remaining references to `:kind` (in clause specs), `emitter_module/0`, or the emitter module path.
- [x] Run `mix format`.
- [x] Run `mix compile`.
- [x] Run focused `mix test` (compiler/spec tests + the prior focused suite used in TG05).
