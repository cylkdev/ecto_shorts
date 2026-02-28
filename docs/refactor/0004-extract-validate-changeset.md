# Extract Repeated Changeset Validation in apply_changeset!/4

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The private function `apply_changeset!/4` in `EctoShorts.CommonSchema` has three case branches (for 3-arity, 2-arity, and 1-arity callback functions). Each branch calls the callback, captures the result as `term`, then runs the identical 5-line validation block that checks whether the result is an `Ecto.Changeset` and raises if not. This validation is repeated verbatim three times.

This refactor extracts the repeated validation into a single `defp validate_changeset!/1` helper. Each branch delegates to it. A change to the validation logic (e.g. improving the error message) then needs to happen in one place instead of three.

To verify behaviour is preserved, run `mix test` from the repository root. All 645 tests and 9 doctests must pass.

## Progress

- [x] (2026-02-28 13:05Z) Wrote RefactorPlan.
- [x] (2026-02-28 13:06Z) Milestone 1: Extracted `validate_changeset!/1`, replaced 3 inline blocks, removed `changeset?/1` and `raise_not_changeset!/1`. `mix format && mix test` - 645 tests, 0 failures.
- [x] (2026-02-28 13:07Z) Fixed 3 credo single-function pipeline warnings by using direct function call syntax instead of pipe.
- [x] (2026-02-28 13:07Z) Milestone 2: Final validation. `mix format` clean. `mix test` - 645 tests, 0 failures. `mix credo --strict` - no new warnings.

## Surprises & Discoveries

- Observation: The initial implementation used pipe syntax (`fun.(...) |> validate_changeset!()`) which triggered 3 new credo warnings about single-function pipelines. Switching to direct call syntax (`validate_changeset!(fun.(...))`) resolved them.
  Evidence: `mix credo --strict` flagged lines 391, 395, 406 with "Use a function call when a pipeline is only one function long."

## Decision Log

- Decision: Merge the two existing helpers `changeset?/1` and `raise_not_changeset!/1` into a single `validate_changeset!/1` that pattern-matches on `%Ecto.Changeset{}` and raises otherwise.
  Rationale: The two helpers are only ever called together in sequence. Combining them into one function eliminates the intermediate boolean check and makes the code more direct.
  Date/Author: 2026-02-28 / Cascade

## Outcomes & Retrospective

The refactor is complete. Three copies of a 5-line `if changeset?(term) do term else raise_not_changeset!(term) end` validation block were replaced with single-line calls to the new `defp validate_changeset!/1`. The two helpers `changeset?/1` and `raise_not_changeset!/1` were removed since their logic is now combined in `validate_changeset!/1`.

The behaviour boundary was fully preserved: all 645 tests and 9 doctests pass. No public API was changed. The same `RuntimeError` with "Expected an Ecto.Changeset, got: ..." is raised for invalid callback returns. `mix format` is clean. `mix credo --strict` shows no new warnings.

File changed: `lib/ecto_shorts/common_schema.ex` - added `defp validate_changeset!/1`, simplified 3 case branches in `apply_changeset!/4`, removed `changeset?/1` and `raise_not_changeset!/1`.

## Context and Orientation

EctoShorts is an Elixir library providing a data-driven API for Ecto. The `EctoShorts.CommonSchema` module at `lib/ecto_shorts/common_schema.ex` provides utility functions for working with Ecto schemas and changesets. The `create_changeset/4` function accepts an optional `:changeset` callback in opts, which is dispatched through `apply_changeset!/4`.

The function `apply_changeset!/4` (lines 387-427) has a `case callback do` with four branches:
- `is_function(fun, 3)` - calls `fun.(schema, data_or_changeset, params)`, validates result
- `is_function(fun, 2)` - calls `fun.(data_or_changeset, params)`, validates result
- `is_function(fun, 1)` - builds a default changeset first, then calls `fun.(changeset)`, validates result
- Catch-all - raises `ArgumentError`

The validation in each of the first three branches is identical: check if `term` is a changeset struct, return it if so, raise if not.

Two existing helpers support this:
- `defp changeset?(%Ecto.Changeset{}), do: true` / `defp changeset?(_), do: false`
- `defp raise_not_changeset!(term)` - raises with an error message

These will be replaced by a single `defp validate_changeset!/1`.

## Behaviour Boundary (Must Remain Unchanged)

- `EctoShorts.CommonSchema.create_changeset/3` and `create_changeset/4` with a `:changeset` option must return the same `Ecto.Changeset` for valid callbacks.
- When a callback returns a non-changeset value, the same `RuntimeError` with message "Expected an Ecto.Changeset, got: ..." must be raised.
- When `:changeset` option is not a function, the same `ArgumentError` must be raised.
- All 645 existing tests pass without modification.

## Code Smell Identified

Duplicate Code, from `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`. The same 5-line validation block (`if changeset?(term) do term else raise_not_changeset!(term) end`) appears 3 times within the same function. This is exact duplication - identical code copied verbatim within a single function body.

Location: `lib/ecto_shorts/common_schema.ex`, lines 392-396, 401-405, and 417-421.

## Refactoring Technique Selected

Extract Function, from `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`. The repeated validation block is extracted into `defp validate_changeset!/1` which pattern-matches on `%Ecto.Changeset{}` to return the value, or raises for any other term. Each case branch calls the callback then pipes through `validate_changeset!/1`.

## Plan of Work

In `lib/ecto_shorts/common_schema.ex`:

1. Add `defp validate_changeset!(%Ecto.Changeset{} = changeset), do: changeset` and `defp validate_changeset!(term), do: raise "Expected an Ecto.Changeset, got: #{inspect(term)}"`.

2. Replace the 3 inline `if changeset?(term) do ... end` blocks with `validate_changeset!(term)`.

3. Remove the now-unused `changeset?/1` and `raise_not_changeset!/1` helpers.

## Concrete Steps

All commands run from: `/Users/kurthogarth/Documents/GitHub/ecto_shorts`

    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix format && mix test
    source ~/.asdf/asdf.sh 2>/dev/null || source $(brew --prefix asdf)/libexec/asdf.sh 2>/dev/null; mix credo --strict

## Validation and Acceptance

Run `mix test` after Milestone 1. All 645 tests and 9 doctests must pass. The refactor preserves all existing changeset creation and validation behaviour.

Run `mix credo --strict` at Milestone 2. No new warnings should appear.

## Idempotence and Recovery

The change is a single file edit. If it fails, revert with `git checkout -- lib/ecto_shorts/common_schema.ex` and retry.

## Artifacts and Notes

(To be filled during execution.)

## Interfaces and Dependencies

In `lib/ecto_shorts/common_schema.ex`, preserve all public `create_changeset` function heads and their return types. Only internal `defp` functions are modified.

## Milestones

### Milestone 1: Extract validate_changeset!/1

Add `defp validate_changeset!/1`. Replace 3 inline validation blocks. Remove unused `changeset?/1` and `raise_not_changeset!/1`. Run `mix format && mix test`. All tests pass.

### Milestone 2: Final validation

Run `mix format`, `mix test`, `mix credo --strict`. All pass. Update Progress and Outcomes.
