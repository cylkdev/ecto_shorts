# Extract Focused Submodules from Actions, CommonFilters, and CommonParams

## Status

Accepted

## Context and Problem Statement

The three largest modules in EctoShorts - `EctoShorts.Actions` (983 lines), `EctoShorts.CommonFilters` (727 lines), and `EctoShorts.CommonParams` (676 lines) - each bundled multiple unrelated responsibilities into a single file. This made it hard to navigate, increased merge-conflict risk on hot files, and forced unrelated logic to change together. For example, `Actions` mixed CRUD operations, batch logic, multi-transaction building, and transaction response handling. A change to batch key normalization required editing the same file as a change to multi-transaction error formatting.

The question was: how should we restructure these modules to reduce their size and improve separation of concerns, without changing the public API that downstream callers depend on?

## Decision Drivers

- **Navigability**: Developers should be able to find logic quickly without scrolling through 700–1000 line files.
- **Single responsibility**: Each module should have one clear reason to change.
- **Public API stability**: The main entry points (`Actions.all`, `Actions.create`, `CommonFilters.convert_params_to_filter`, `CommonParams.convert_to_insert_params`, etc.) must remain unchanged in name, arity, and return shape.
- **Test preservation**: All 645 existing tests must continue to pass without modification.
- **No new dependencies**: The refactor must use only existing Elixir/OTP mechanisms.

## Considered Options

1. **Extract focused submodules** (chosen): Create child modules under each namespace (`Actions.Multi`, `Actions.Batch`, `CommonFilters.BindingParams`, `CommonParams.Timestamps`, `CommonParams.Placeholders`) and move cohesive groups of private functions there. The parent modules delegate to the children internally. Replace repetitive function-clause dispatch in `CommonFilters` with a map-based lookup. Consolidate duplicated `get_query_fields/2` into `CommonSchema`.

2. **Inline refactor only**: Keep all code in the same modules but restructure internally - extract private helper functions, rename for clarity, reorder for readability. This would not reduce module size and would not address the divergent-change smell.

3. **Behaviour-based dispatch for CommonFilters**: Define a `QueryBuilder` behaviour and have each filter module implement it. Use a registry or config to dispatch. This would add significant abstraction overhead for a pattern that is stable and unlikely to gain new filter types frequently.

## Decision Outcome

Chosen option: **Extract focused submodules**, because it directly reduces module size, isolates unrelated concerns into separate files, and preserves the public API through simple internal delegation. It avoids the over-engineering risk of a behaviour-based approach while being more effective than inline-only restructuring.

### Consequences

**Good:**

- `actions.ex` reduced from 983 to 614 lines (−38%).
- `common_filters.ex` reduced from 727 to 543 lines (−25%).
- `common_params.ex` reduced from 676 to 505 lines (−25%).
- Five new focused submodules, each under 250 lines.
- Duplicated `get_query_fields/2` consolidated into one location (`CommonSchema`).
- 15 repetitive `apply_query_builder` clauses replaced with a 15-entry map and a 3-line dispatch function.
- All 645 tests pass without modification.

**Neutral:**

- Total line count across all affected files increased slightly due to module boilerplate (`defmodule`, `alias`, `@moduledoc false`). This is expected for any extraction.
- `reduce_filter_params/6` in `CommonFilters` was promoted from `defp` to `@doc false def` so `BindingParams` can call back into it. This is an internal-only API change.

**Bad:**

- More files to navigate when tracing the full call path of a multi-transaction operation (now spans `actions.ex` -> `actions/multi.ex`).
- `actions.ex` at 614 lines is still above the 400-line ideal. Further extraction is possible but was out of scope.

## Validation

Run from the repository root:

    export PATH="$HOME/.asdf/shims:$PATH"
    mix test

Expected output: `9 doctests, 645 tests, 0 failures`.

Additionally, `mix credo --strict` passes with no new warnings, and `mix dialyzer` shows only a pre-existing error in `dynamics.ex` unrelated to this change.

## When to Revisit

- If `actions.ex` grows beyond 700 lines again, consider extracting CRUD, bulk, and transaction helpers into their own submodules.
- If new query filter types are added frequently to `CommonFilters`, consider whether the map-based dispatch should be promoted to a behaviour-based registry.
- If `CommonParams` gains new insert-entry normalization paths, the `normalize_insert_entry` dispatch chain may benefit from extraction.

## More Information

- RefactorPlan: `docs/refactor/0001-full-codebase-refactor.md`
- Code smell references: `.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`, `.agent/refactor/code_smells/change_preventers/DIVERGENT_CHANGE.md`, `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`
- Technique references: `.agent/refactor/techniques/moving_features_between_modules/EXTRACT_MODULE.md`, `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`
