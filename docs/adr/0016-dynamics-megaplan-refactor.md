# Remove duplicate normalize_entries, inline build_field_predicate, extract build_subquery

---
Status: accepted
Date: 2026-03-05
Deciders: [cascade]
Consulted: []
Informed: []
---

## Context and Problem Statement

After the 0015–0017 refactor series, `lib/ecto_shorts/dynamics.ex` had three residual structural problems.

First, the `normalize_entries/2` function was copied verbatim across `dynamics.ex` (lines 247–284) and `dynamics/postgres.ex`. A future bug fix or extension would have to be applied twice, and it was not obvious which copy was canonical.

Second, `build_field_predicate/7` had a single call site inside `append_predicates/6` and added no domain meaning beyond the body it wrapped. It was a Middle Man: a function that did nothing except delegate to two other functions.

Third, the `:from`-pop + `put_default_select` + `CommonFilters.convert_params_to_filter` sequence was duplicated between `resolve_exists_payload/3` (the `is_list` branch) and `resolve_subquery_payload/4` (the `Keyword.keyword?/has_key?(:from)` branch), making the shared subquery-building logic invisible.

## Decision Drivers

- A function should live in exactly one place; duplication forces readers to track two canonical copies and multiplies the cost of any future change.
- A function with a single call site that adds no domain meaning should be inlined; indirection is only worth its cost when it names a concept or is reused.
- Shared logic should be extracted into a named helper so the pattern is visible and the callers become one-liners.

## Considered Options

1. **Remove duplication in place**: delete `normalize_entries/2` from `dynamics.ex`, inline `build_field_predicate/7`, extract `build_subquery/4`. (chosen)
2. **Extract to a shared module** (`EctoShorts.Dynamics.ExprNormalizer`): move `normalize_entries/2` to a new module and alias it from both callers.
3. **Leave as-is**: accept the three smells and revisit only when they cause a visible bug or merge conflict.

## Decision Outcome

Chosen option: **Remove duplication in place**, because it eliminates all three smells with the minimum surface change — one file, no new modules, no public API change, and the adapter (`postgres.ex`) already owns `normalize_entries/2` which is the correct ownership boundary.

### Consequences

Good, because `normalize_entries/2` now lives only in `postgres.ex` where it belongs as an adapter-specific concern. Any future change to entry normalization has one canonical home.

Good, because `append_predicates/6` is now self-contained: readers can trace the full field-dispatch logic in one function without jumping to `build_field_predicate/7`.

Good, because `build_subquery/4` makes the shared subquery-building pattern explicit and named. Both `resolve_exists_payload/3` and `resolve_subquery_payload/4` become structural routers with one-liner branches.

Bad, because `append_predicates/6` is now longer (the inlined body adds ~12 lines). A reader who wants to understand field dispatch must read the full `cond` rather than following a named helper. This is an acceptable tradeoff because the inlined body is straightforward and the helper name added no additional meaning.

## Validation

Run the full test suite from the repository root:

    mix test --seed 0 --trace

Expected output: `9 doctests, 884 tests, 0 failures`.

Additionally, confirm the structural changes by inspecting `lib/ecto_shorts/dynamics.ex`:

- `normalize_entries` must not appear anywhere in the file (search: `grep normalize_entries lib/ecto_shorts/dynamics.ex` should return nothing).
- `build_field_predicate` must not appear anywhere in the file.
- `build_subquery` must appear as a `defp` definition.
- `resolve_exists_payload` list branch must call `build_subquery`.
- `resolve_subquery_payload` keyword branch must call `build_subquery`.

## Pros and Cons of the Options

### Remove duplication in place (chosen)

Deletes `normalize_entries/2` from `dynamics.ex` and updates `expand_and_reduce/7` to iterate raw `{op, val}` keyword entries directly. Inlines `build_field_predicate/7` body into both call sites in `append_predicates/6`. Adds `build_subquery/4` private function and updates two one-line branches.

Good, because no new modules or public APIs are introduced.
Good, because the adapter boundary is reinforced: only `postgres.ex` knows about entry normalization.
Bad, because `append_predicates/6` becomes slightly longer after the inline.

### Extract to a shared module

Create `EctoShorts.Dynamics.ExprNormalizer` and alias it from both `dynamics.ex` and `postgres.ex`.

Good, because neither caller would own the function.
Bad, because a new module for a single 38-line function is over-engineering.
Bad, because `normalize_entries/2` is an adapter-specific concern; making it shared contradicts the "adapter owns normalization" boundary established in refactor 0015.

### Leave as-is

No code changes.

Good, because zero risk of regression.
Bad, because the duplication will compound as the codebase grows, and a reader cannot determine which copy is authoritative.

## More Information

Related refactor plans:

- `docs/refactor/0015-dynamics-thin-dispatcher.md` — established the "adapter normalizes, Dynamics dispatches" boundary.
- `docs/refactor/0016-postgres-adapter-cond-dispatch.md` — consolidated cond-based dispatch in the Postgres adapter.
- `docs/refactor/0017-fix-normalize-params-coverage.md` — fixed bugs in the replacement normalization function.

The megaplan RefactorPlan is at `/Users/kurthogarth/.windsurf/plans/dynamics-megaplan-a97504.md`.

Revisit this decision if a second database adapter is added (at that point a shared normalization module may become appropriate) or if `append_predicates/6` grows beyond ~60 lines (at which point re-extracting a named helper may be warranted).
