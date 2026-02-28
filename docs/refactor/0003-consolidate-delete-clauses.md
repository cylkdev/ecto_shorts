# Consolidate Duplicate delete/2 Clauses in Actions

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The `EctoShorts.Actions.delete/2` function has two clauses (for changesets and schema structs) that contain identical logic: extract the schema module, create a changeset via `CommonSchema.create_changeset`, call `Config.repo!(opts).delete`, and wrap any `{:error, changeset}` with `Error.call(:conflict, ...)`. The only difference is the pattern match used to extract the `schema` atom. Since `CommonSchema.create_changeset/3` already accepts both changesets and schema structs, these two clauses can be unified by extracting the shared body into a single private helper.

After this refactor, a bug fix to the delete error-wrapping logic needs to be made in one place instead of two. The public `delete/2` API, return values, and error shapes are unchanged.

To verify behaviour is preserved, run `mix test` from the repository root. All 645 tests and 9 doctests must pass.

## Progress

- [x] (2026-02-28 13:00Z) Wrote RefactorPlan.
- [x] (2026-02-28 13:01Z) Milestone 1: Extracted `do_delete/3`, consolidated both `delete/2` clause bodies. `mix format && mix test` — 645 tests, 0 failures.
- [x] (2026-02-28 13:02Z) Milestone 2: Final validation. `mix format` clean. `mix test` — 645 tests, 0 failures. `mix credo --strict` — no new warnings.

## Surprises & Discoveries

- Observation: The refactor is net +1 mods/funs in credo (736 vs 735) because we added one `defp do_delete/3` while not removing any function heads — we only shortened the bodies of the two existing clauses.
  Evidence: `mix credo --strict` reports 736 mods/funs.

## Decision Log

- Decision: Extract a private `do_delete/3` that takes any schema-bearing data (changeset or struct), the schema module, and opts.
  Rationale: Both existing clauses perform identical operations after extracting the schema. The pattern match that extracts the schema remains in the caller clause heads; only the body is shared.
  Date/Author: 2026-02-28 / Cascade

## Outcomes & Retrospective

The refactor is complete. Two near-identical `delete/2` clause bodies (each 16 lines) were replaced with single-line delegations to the new `defp do_delete/3` helper. This eliminates 15 lines of exact duplication and ensures that any future change to delete error-wrapping logic only needs to happen in one place.

The behaviour boundary was fully preserved: all 645 tests and 9 doctests pass. No public API was changed. The same error shapes (`:conflict` code, "failed to delete record." message, `%{schema: _, changeset: _}` details) are returned for both changeset and schema struct inputs. `mix format` is clean. `mix credo --strict` shows no new warnings.

File changed: `lib/ecto_shorts/actions.ex` — added `defp do_delete/3`, simplified two `delete/2` clause bodies.

## Context and Orientation

EctoShorts is an Elixir library providing a data-driven API for Ecto. The `EctoShorts.Actions` module at `lib/ecto_shorts/actions.ex` is the public entry point for CRUD operations. The `delete/2` function handles deletion of records, changesets, and lists thereof. It has multiple clauses dispatching by input type.

The two clauses in question are at lines 210-226 (changeset input) and 228-244 (schema struct input). Both follow the same algorithm:

1. Create a changeset via `CommonSchema.create_changeset(schema, input, opts)`
2. Call `Config.repo!(opts).delete(opts)`
3. If `{:error, failed_changeset}`, wrap with `Error.call(:conflict, "failed to delete record.", ...)`
4. If `{:ok, record}`, pass through

Key modules:
- `lib/ecto_shorts/actions.ex` — the file being refactored
- `lib/ecto_shorts/common_schema.ex` — provides `create_changeset/3` which handles both changesets and structs
- `lib/ecto_shorts/actions/error.ex` — provides `Error.call/4` for error wrapping

## Behaviour Boundary (Must Remain Unchanged)

- `EctoShorts.Actions.delete/1` returns `{:ok, record}` or `{:error, error}` for a single record/changeset.
- `EctoShorts.Actions.delete/2` with a changeset and opts returns `{:ok, record}` or `{:error, error}` with the same error shape (`:conflict` code, "failed to delete record." message, `%{schema: _, changeset: _}` details).
- `EctoShorts.Actions.delete/2` with a schema struct and opts returns the same shape.
- `EctoShorts.Actions.delete/2` with a list returns `{:ok, [record]}` or `{:error, reason}`.
- `EctoShorts.Actions.delete/3` with queryable, id, opts returns `{:ok, record}` or `{:error, error}`.
- All 645 existing tests pass without modification.

## Code Smell Identified

Duplicate Code, from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`. Two function clauses in the same module contain identical logic, differing only in the pattern match. Signs: "I've seen this code somewhere before" feeling, bug fixes that must be applied in two places.

Location: `lib/ecto_shorts/actions.ex`, lines 210-226 and 228-244.

## Refactoring Technique Selected

Extract Function, from `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`. The shared body (changeset creation, repo delete, error wrapping) is extracted into a private helper `do_delete/3`. Each existing clause retains its pattern match and delegates to the helper.

## Plan of Work

In `lib/ecto_shorts/actions.ex`:

1. Add a new private function `do_delete(schema_data, schema, opts)` after the existing `delete` clauses. This function contains the `with {:error, failed_changeset} <- ...` body that is currently duplicated.

2. Replace the body of the changeset clause (line 210) with a call to `do_delete(changeset, schema, opts)`.

3. Replace the body of the schema struct clause (line 228) with a call to `do_delete(schema_struct, schema, opts)`.

## Concrete Steps

All commands run from: `/Users/kurthogarth/Documents/GitHub/ecto_shorts`

    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix format && mix test
    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix credo --strict

## Validation and Acceptance

Run `mix test` after Milestone 1. All 645 tests and 9 doctests must pass. The refactor preserves all existing delete behaviour.

Run `mix credo --strict` at Milestone 2. No new warnings should appear.

## Idempotence and Recovery

The change is a single file edit. If it fails, revert with `git checkout -- lib/ecto_shorts/actions.ex` and retry.

## Artifacts and Notes

(To be filled during execution.)

## Interfaces and Dependencies

In `lib/ecto_shorts/actions.ex`, preserve all public `delete` function heads and their return types. Introduce only `defp do_delete/3` as an internal helper.

## Milestones

### Milestone 1: Extract do_delete/3 and consolidate

Extract the shared delete body into `defp do_delete/3`. Replace both clause bodies. Run `mix format && mix test`. All tests pass.

### Milestone 2: Final validation

Run `mix format`, `mix test`, `mix credo --strict`. All pass. Update Progress and Outcomes.
