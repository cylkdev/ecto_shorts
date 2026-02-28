# Consolidate Duplicate preload_change_assoc in CommonChanges

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The `EctoShorts.CommonChanges` module has two definitions of `preload_change_assoc`: a 3-arity version (with opts) and a 2-arity version (without opts). The 2-arity version duplicates the core branching logic of the 3-arity version — both check `Map.has_key?(changeset.params, Atom.to_string(key))` and then either preload+put_or_cast the association or cast it. The 2-arity is functionally identical to calling the 3-arity with `opts = []`.

This refactor merges the two by adding `opts \\ []` to the 3-arity head and deleting the 2-arity clause. This eliminates 8 lines of duplicated code and ensures future changes to the preload-change logic happen in one place.

To verify behaviour is preserved, run `mix test` from the repository root. All 645 tests and 9 doctests must pass.

## Progress

- [x] (2026-02-28 13:10Z) Wrote RefactorPlan.
- [x] (2026-02-28 13:11Z) Milestone 1: Added `opts \\ []` default, deleted 2-arity clause and its `@spec`. `mix format && mix test` — 645 tests, 0 failures.
- [x] (2026-02-28 13:11Z) Milestone 2: Final validation. `mix format` clean. `mix test` — 645 tests, 0 failures. `mix credo --strict` — no new warnings. Credo `mods/funs` dropped from 735 to 734.

## Surprises & Discoveries

(None yet.)

## Decision Log

- Decision: Merge the 2-arity into the 3-arity with a default argument rather than keeping both.
  Rationale: The 3-arity body works correctly with `opts = []` because `opts[:required_when_missing]` returns nil (so `required?` becomes `false`), `Keyword.put([], :required, false)` produces `[required: false]`, and all downstream functions (`preload_changeset_assoc/3`, `put_or_cast_assoc/3`, `Changeset.cast_assoc/3`) already accept keyword opts including empty lists.
  Date/Author: 2026-02-28 / Cascade

## Outcomes & Retrospective

The refactor is complete. The duplicate 2-arity `preload_change_assoc/2` clause (8 lines) was eliminated by adding `opts \\ []` to the 3-arity definition. Callers using either arity continue to work identically.

The behaviour boundary was fully preserved: all 645 tests and 9 doctests pass. No public API was changed — `preload_change_assoc(changeset, key)` and `preload_change_assoc(changeset, key, opts)` both resolve to the same function. `mix format` is clean. `mix credo --strict` shows no new warnings.

File changed: `lib/ecto_shorts/common_changes.ex` — merged 2-arity into 3-arity with default argument.

## Context and Orientation

EctoShorts is an Elixir library providing a data-driven API for Ecto. The `EctoShorts.CommonChanges` module at `lib/ecto_shorts/common_changes.ex` provides helper functions for managing changesets and associations.

The function `preload_change_assoc` is a public function used in schema changeset functions to preload and cast associations. It exists in two forms:

- `preload_change_assoc(changeset, key, opts)` (line 249) — handles `:required_when_missing` option, normalizes opts, then branches on whether the association key is present in changeset params.
- `preload_change_assoc(changeset, key)` (line 269) — same branching logic without opts, calling the same downstream functions without options.

The downstream functions already accept optional keyword lists: `preload_changeset_assoc(changeset, key, opts \\ [])` and `put_or_cast_assoc(changeset, key, opts \\ [])`.

## Behaviour Boundary (Must Remain Unchanged)

- `EctoShorts.CommonChanges.preload_change_assoc(changeset, key)` must produce the same changeset as before — preloading and casting (or just casting) the association based on whether the key is present in params.
- `EctoShorts.CommonChanges.preload_change_assoc(changeset, key, opts)` must handle `:required_when_missing` and `:required` opts identically.
- All 645 existing tests pass without modification.

## Code Smell Identified

Duplicate Code, from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`. The same `if Map.has_key?(changeset.params, Atom.to_string(key))` branching pattern is duplicated between the 2-arity and 3-arity versions. This is structural duplication — same structure with different (default) values.

Location: `lib/ecto_shorts/common_changes.ex`, lines 249-266 and 269-277.

## Refactoring Technique Selected

Inline Function / default argument, from `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`. The 2-arity clause is eliminated by making opts optional in the 3-arity definition.

## Plan of Work

In `lib/ecto_shorts/common_changes.ex`:

1. Change the 3-arity `@spec` to include `keyword()` as optional.
2. Add `opts \\ []` default to the 3-arity function head.
3. Delete the 2-arity clause (lines 268-277) and its `@spec`.

## Concrete Steps

All commands run from: `/Users/kurthogarth/Documents/GitHub/ecto_shorts`

    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix format && mix test
    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix credo --strict

## Validation and Acceptance

Run `mix test` after Milestone 1. All 645 tests and 9 doctests must pass.

Run `mix credo --strict` at Milestone 2. No new warnings should appear.

## Idempotence and Recovery

The change is a single file edit. If it fails, revert with `git checkout -- lib/ecto_shorts/common_changes.ex` and retry.

## Artifacts and Notes

(To be filled during execution.)

## Interfaces and Dependencies

In `lib/ecto_shorts/common_changes.ex`, the public function signature changes from two separate definitions to one with an optional third argument. Callers using either `preload_change_assoc(changeset, key)` or `preload_change_assoc(changeset, key, opts)` continue to work identically.

## Milestones

### Milestone 1: Merge 2-arity into 3-arity with default opts

Add `opts \\ []` to the 3-arity definition. Delete the 2-arity clause and its `@spec`. Run `mix format && mix test`. All tests pass.

### Milestone 2: Final validation

Run `mix format`, `mix test`, `mix credo --strict`. All pass. Update Progress and Outcomes.
