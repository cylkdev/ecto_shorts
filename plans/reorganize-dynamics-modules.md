# Reorganize Query Composition Modules

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. Maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

The query composition modules have grown or been refactored to the point where module names no longer reflect their responsibilities, thin wrapper modules add indirection without value, and the `Adapters.Postgres` module sits in the wrong namespace with a mismatched file path. After this change, the module tree accurately describes what each module does: `EctoShorts.Adapter.Dynamic` is a documented behaviour for building dynamic expressions; `EctoShorts.Dynamics.Postgres` is the concrete Postgres implementation; `EctoShorts.QueryBinding` generates compile-time Ecto query binding patterns; `EctoShorts.Adapter.QueryBuilder` and `EctoShorts.Adapter.QueryProvider` are documented public API contracts. The test file tree matches the lib file tree.

## In Scope

1. Create `EctoShorts.Adapter.Dynamic` as a new behaviour module at `lib/ecto_shorts/dynamics/adapter.ex`, documenting what an adapter is, what `build_dynamic/4` does, and how to implement one. Delete `lib/ecto_shorts/dynamic_expr_builder.ex`.
2. Move `lib/ecto_shorts/dynamics/postgres.ex` (module `EctoShorts.Adapters.Postgres`) to `lib/ecto_shorts/dynamics/adapters/postgres.ex` and rename the module to `EctoShorts.Dynamics.Postgres`. Update `@behaviour` to reference `EctoShorts.Adapter.Dynamic`.
3. Move the three sub-modules: `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`, `array_expr.ex`, `common_expr.ex` to `lib/ecto_shorts/dynamics/adapters/postgres/` and rename them to `EctoShorts.Dynamics.Postgres.{ScalarExpr,ArrayExpr,CommonExpr}`.
4. Move test files: `test/ecto_shorts/dynamics/postgres_test.exs` and `test/ecto_shorts/dynamics/postgres/` to `test/ecto_shorts/dynamics/adapters/` and update module names.
5. Create `lib/ecto_shorts/query_bindings.ex` with module `EctoShorts.QueryBinding`, containing `query_binding_contracts/2` from `Compiler` plus all functions from `EctoShorts.Dynamics.Helpers`. Delete `lib/ecto_shorts/compiler.ex` and `lib/ecto_shorts/dynamics/helpers.ex`.
6. Add `@moduledoc` and function docs to `EctoShorts.Adapter.QueryProvider`.
7. Add `@moduledoc` and `@callback` docs to `EctoShorts.Adapter.QueryBuilder`. Inline the 1-line `QueryBuilder.build_query/7` dispatcher call directly in `EctoShorts.CommonFilters`.
8. Update all call sites: aliases, `@behaviour`, `config/config.exs`, `mix.exs` docs groups.

## Out of Scope

- Changes to function signatures, public API behaviour, or test logic.
- Any changes to `EctoShorts.Dynamics` top-level module (it is already correct).
- Changes to test support helper modules (`TestQueryProvider`, `TestNoOpQueryProvider`, etc.) - their names are generic and do not need renaming.

## Progress

- [ ] Create ExecPlan
- [ ] Create `EctoShorts.Adapter.Dynamic` behaviour; delete `dynamic_expr_builder.ex`
- [ ] Move+rename `Adapters.Postgres` → `Dynamics.Adapters.Postgres` and 3 sub-modules; delete old lib files
- [ ] Move+rename 4 test spec files; delete old test files
- [ ] Create `QueryBinding` absorbing `Compiler` + `Dynamics.Helpers`; delete both old files
- [ ] Add docs to `QueryProvider`
- [ ] Add docs to `QueryBuilder`; inline dispatcher in `CommonFilters`
- [ ] Update all remaining call sites
- [ ] `mix test` green

## Milestones

### Milestone 1 - New behaviour + adapter namespace

Create `EctoShorts.Adapter.Dynamic` behaviour. Move `EctoShorts.Adapters.Postgres` and its three sub-modules into the new `Dynamics.Adapters.Postgres` namespace. Move matching test files. Delete old files.

Verify: `mix test` passes after each file move.

### Milestone 2 - QueryBinding (replaces Compiler + Helpers)

Create `lib/ecto_shorts/query_bindings.ex` with module `EctoShorts.QueryBinding` holding all functions from both `Compiler` and `Dynamics.Helpers`. Update every file that aliases `EctoShorts.Compiler` or `EctoShorts.Dynamics.Helpers`. Delete both old files.

Verify: `mix test` passes.

### Milestone 3 - Docs and dispatcher inline

Add `@moduledoc` to `QueryProvider` and `QueryBuilder`. Inline the `QueryBuilder.build_query/7` dispatcher into `CommonFilters`. Update `mix.exs` docs groups.

Verify: `mix test` passes (11 doctests + 721 tests, 0 failures).

## Surprises & Discoveries

(none yet)

## Decision Log

- Decision: `EctoShorts.Compiler` renamed to `EctoShorts.QueryBinding`.
  Rationale: The module only generates Ecto query binding patterns - "Compiler" implies general compilation, which is misleading.
  Date/Author: 2026-03-15

- Decision: `EctoShorts.Dynamics.Helpers` functions absorbed into `QueryBinding`.
  Rationale: All helpers are exclusively used at compile time by the same adapter modules that call `query_binding_contracts`. Keeping them in one module eliminates an alias hop.
  Date/Author: 2026-03-15

- Decision: `QueryProvider` and `QueryBuilder` kept as public API modules with docs added.
  Rationale: Both define extension contracts used by callers outside the library. Deleting them would break public API.
  Date/Author: 2026-03-15

## Outcomes & Retrospective

(to be filled at completion)

## Context and Orientation

Key files and their roles:

- `lib/ecto_shorts/dynamic_expr_builder.ex` - `EctoShorts.DynamicExprBuilder`: thin behaviour that will be replaced by `Dynamics.Adapter`.
- `lib/ecto_shorts/dynamics/postgres.ex` - `EctoShorts.Adapters.Postgres`: the Postgres dynamic expression adapter. Module name/path mismatch.
- `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`, `array_expr.ex`, `common_expr.ex` - sub-modules of the adapter, will move to `dynamics/adapters/postgres/`.
- `lib/ecto_shorts/compiler.ex` - `EctoShorts.Compiler`: generates binding pattern ASTs. Will become `EctoShorts.QueryBinding`.
- `lib/ecto_shorts/dynamics/helpers.ex` - `EctoShorts.Dynamics.Helpers`: AST helper functions used at compile time. Will merge into `QueryBinding`.
- `lib/ecto_shorts/query_builder.ex` - `EctoShorts.Adapter.QueryBuilder`: public behaviour + 1-line dispatcher.
- `lib/ecto_shorts/query_provider.ex` - `EctoShorts.Adapter.QueryProvider`: public dispatcher for query provider modules.

## Plan of Work

See steps in "In Scope" above and "Milestones". Each step: create new file with correct content, update all aliases pointing to old name, delete old file, run `mix test`.

## Validation and Acceptance

Run `mix test` from repo root. Expect: `11 doctests, 721 tests, 0 failures`. No references to `EctoShorts.Adapters.Postgres`, `EctoShorts.DynamicExprBuilder`, `EctoShorts.Dynamics.Helpers`, or `EctoShorts.Compiler` should remain in `lib/` or `test/`.
