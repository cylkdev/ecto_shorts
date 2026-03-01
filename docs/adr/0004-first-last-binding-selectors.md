# Add `:first` and `:last` as flat binding selector modes

---
Status: accepted
Date: 2026-02-28
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

The `%{bind: ...}` syntax in `EctoShorts.CommonFilters` lets callers target a specific query binding by name (`:as`) or by position (`:at`). Both modes require the caller to know the exact alias or integer position of the binding they want to target. In practice, two positions come up repeatedly: the root `from` binding (always position 1) and the last join binding (position varies by query). Requiring callers to hard-code `%{bind: %{at: %{1 => ...}}}` or compute the last position themselves adds friction for these common cases.

## Decision Drivers

* Callers should not need to compute or hard-code positional indices for the two most common binding targets.
* The new syntax must not collide with the existing `:first` and `:last` query operation filters (which are top-level keys, not inside `%{bind: ...}`).
* The implementation must reuse the existing compiled clause dispatch system without requiring changes to `EctoShorts.Compiler` or `QueryBindingBuilder`.
* The new modes must work everywhere existing binding selectors work: where filters, order_by, group_by, having, distinct, select, windows, update, and preload.

## Considered Options

1. **New flat binding modes** - `:first` and `:last` become sibling modes alongside `:as` and `:at` inside `%{bind: ...}`. They are "flat" because their params are the filter parameters directly (no `{target, params}` nesting).
2. **Special `:at` targets** - `:first` and `:last` are atom values inside the existing `:at` mode, e.g. `%{bind: %{at: %{:first => ...}}}`. Resolved at runtime to the actual position.
3. **Both syntaxes** - Support both option 1 and option 2.

## Decision Outcome

Chosen option: "New flat binding modes", because it is the simplest syntax for the caller, avoids overloading the `:at` mode with non-integer keys, and keeps the implementation localized to `BindingParams` without touching the Compiler or dynamic expression adapters.

### Consequences

Good, because callers can write `%{bind: %{first: %{published: true}}}` instead of `%{bind: %{at: %{1 => %{published: true}}}}`, reducing boilerplate for the most common binding targets.

Good, because the implementation resolves `:first` to `{:at, 1}` and `:last` to `{:at, N}` before reaching compiled clauses, so no changes are needed to the Compiler, QueryBindingBuilder, or dynamic expression adapters.

Bad, because `:last` resolution depends on `max_binding_positions`. If a query has more bindings than the compiled maximum, the resolved `{:at, N}` will not match any compiled clause. This is the same constraint that applies to using `{:at, N}` directly.

## Validation

Run the binding selector tests and confirm the new `:first` and `:last` tests pass:

    mix test test/ecto_shorts/common_filters/binding_and_boolean_test.exs --seed 0 --trace

Look for the describe blocks `"convert_params_to_filter/3 :first binding selector"` and `"convert_params_to_filter/3 :last binding selector"`. All tests in those blocks must pass.

Verify no existing tests broke:

    mix test --seed 0

## Pros and Cons of the Options

### New flat binding modes

`:first` and `:last` are new keys at the same level as `:as` and `:at` inside `%{bind: ...}`. They wrap filter params directly without a target key.

Good, because the syntax is concise: `%{bind: %{first: %{...}}}`.
Good, because implementation is localized to `BindingParams` - one new private function `reduce_flat_bind_params/6` and one resolver `resolve_flat_binding/2`.
Good, because no collision with existing `:first`/`:last` query operation filters since those are top-level keys.
Bad, because it adds two new atoms to the binding mode vocabulary that callers must learn.

### Special `:at` targets

`:first` and `:last` are atom targets inside the existing `:at` mode, e.g. `%{bind: %{at: %{:first => %{...}}}}`.

Good, because no new binding modes are introduced.
Bad, because the syntax is more verbose: `%{bind: %{at: %{:first => %{...}}}}`.
Bad, because it overloads `:at` to accept both integer and atom keys, making validation more complex.

### Both syntaxes

Support `:first`/`:last` as both new binding modes and as `:at` target values.

Good, because callers can choose whichever syntax they prefer.
Bad, because two ways to do the same thing increases cognitive load and documentation surface.

## More Information

The `:first` mode always resolves to `{:at, 1}` (pure alias). The `:last` mode resolves at runtime to `{:at, CommonQuery.query_binding_count(query)}`. For a bare schema with no joins, `:last` targets position 1 (the `from` binding).

Files changed:
* `lib/ecto_shorts/common_filters/binding_params.ex` - core resolution logic
* `lib/ecto_shorts/common_filters/order_by.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters/group_by.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters/windows.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters/select.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters/distinct.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters/update.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters/preload.ex` - pass query to `normalize_bind_params`
* `lib/ecto_shorts/common_filters.ex` - moduledoc binding selector section updated
* `guides/RULES.md` - Rule 16 updated
* `test/ecto_shorts/common_filters/binding_and_boolean_test.exs` - new tests

Revisit this decision if the binding selector system is extended with additional resolution strategies (e.g. relative offsets like `{:at, -1}` for "second to last").
