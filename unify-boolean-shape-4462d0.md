# Merge dynamics.ex logic into Postgres adapter and fix boolean combiners

Move all logic from `dynamics.ex` into the Postgres adapter so boolean combining and `normalize_entries` live in the same module. `dynamics.ex` becomes a thin entrypoint that resolves the adapter and delegates.

---

## Phase 1: Initial bug fixes (COMPLETED)

These fixes were implemented and all tests pass (884 tests, 9 doctests, 0 failures).

### Fix A — Field-level `:and`/`:or` returns nil (12 failures)

**Root cause:** When `%{views: %{or: [>: 10, <: 5]}}` is processed, the `:or` key inside the field's keyword list produces `{:views, {:or, [>: 10, <: 5]}}` — a shape no handler recognized. The adapter received `{:or, [>: 10, <: 5]}` as the expression and returned nil.

**Fix applied:** Added a function clause (C2) that matches `{key, {boolean_op, sub_entries}}` and reduces over sub-entries with the boolean merge. This was the initial fix that got all tests passing.

### Fix B — `adapter_for_repo!` fails for lazy-loaded adapter modules (2 failures)

**Root cause:** `function_exported?/3` called without `Code.ensure_loaded?/1` first.

**Fix applied:** Added `Code.ensure_loaded?(builder)` before the `function_exported?` check.

### Fix C — Keyword branches unified (COMPLETED — superseded by Phase 3)

Replaced duplicate keyword-iteration `Enum.reduce` blocks with `{key, {:and, value}}` delegation through C2. Added `merge_dynamic(dyn_left, _, nil)` guard for warn-and-skip safety.

Note: This intermediate fix produces `{field, {:and, entries}}` which Phase 3 eliminates. The `{key, {:and, value}}` shape and C2 are both removed in Phase 3.

---

## Phase 2: Design analysis (COMPLETED)

Systematic review of internal shapes identified the core design problem.

### The problem

The shape `{field, {bool_op, entries}}` (e.g., `{:views, {:or, [>: 10, <: 5]}}`) requires a dedicated clause (C2) that reorders it to `{bool_op, [{field, e1}, {field, e2}]}`. This reordering is a design smell.

### Root cause: R5 hardcodes `:and`

When `:or` is the outer combiner, the keyword branch (R5) wraps as `{key, {:and, value}}` — hardcoding `:and` regardless of context. The outer `:or` merge at R4 has only one entry to merge (the whole field), so OR never fires on individual operators.

### What `:and` and `:or` actually are

`:and` and `:or` are **combiners**. They control how flattened entries are merged. The data inside is always flattened the same way — to individual `{field, {op, val}}` entries. The combiner decides AND vs OR merge.

Example: `%{or: %{views: [>: 10, <: 5]}}`
- Data: `%{views: [>: 10, <: 5]}` → flattens to `[{:views, {:>, 10}}, {:views, {:<, 5}}]`
- Combiner: `:or` → merge entries with OR
- Result: `views > 10 OR views < 5`

### Input shape change

Old: `%{views: %{or: [>: 10, <: 5]}}` — field wraps bool_op (creates `{field, {bool_op, entries}}`)
New: `%{or: %{views: [>: 10, <: 5]}}` — bool_op outermost (bool_op controls merging)

---

## Phase 3: Architecture decisions (READY FOR IMPLEMENTATION)

### Hard rules

1. **Never reorder keys in a tuple** — no clause may match `{a, b}` and produce `{b, a}`
2. **Never produce `{field, {bool_op, entries}}`** — this shape is the problem being fixed
3. **Prefer recursion** — no `Enum.flat_map` or inline flattening
4. **`normalize_entries` is adapter responsibility** — stays in Postgres adapter
5. **All design decisions require user approval**

### Architecture

**`dynamics.ex` keeps (thin entrypoint):**
- `convert_to_dynamic/4` — public API, same signature
- `adapter_for_repo!/1` — resolve which adapter to use
- Delegates to `adapter.convert_to_dynamic/4`

**Postgres adapter absorbs:**
- `convert_to_dynamic/4` — new, absorbs `append_predicates`, `merge_dynamic`, boolean combining
- `build_dynamic/4` — existing
- `normalize_entries` — existing, used by R4 to expand field keyword entries before reducing with the combiner
- `reduce_entries` — existing

### The fix for R5 hardcoding `:and`

With logic in the adapter, R4 (boolean combiner handler) can call `normalize_entries` to expand `{field, keyword_list}` entries into individual `{field, {op, val}}` entries before reducing with the combiner. No hardcoded `:and` needed.

Example: `%{or: %{views: [>: 10, <: 5]}}`
- Unwraps to `{:or, [views: [>: 10, <: 5]]}`
- R4: `:or` is combiner, value is keyword list
- R4: entry `{:views, [>: 10, <: 5]}` → `normalize_entries` expands to `[{:>, 10}, {:<, 5}]`
- R4: wraps each with field → `[{:views, {:>, 10}}, {:views, {:<, 5}}]`
- R4: reduces with `:or` merge → `views > 10 OR views < 5`

---

## Scope

### Files to change

1. `lib/ecto_shorts/dynamics.ex` — strip to thin entrypoint
2. `lib/ecto_shorts/dynamics/postgres.ex` — absorb `append_predicates`, `merge_dynamic`, boolean combining
3. `lib/ecto_shorts/dynamic.ex` — update behaviour to include `convert_to_dynamic/4`
4. `guides/RULES.md` — update Rules 12/13 with new input shapes
5. `guides/WORKED_EXAMPLES.md` — update field-level boolean examples
6. `guides/DESIGN.md` — update with final architecture
7. Tests — update test inputs to use new shapes

### Callers of `Dynamics.convert_to_dynamic/4` (no change needed — module name stays)

- `lib/ecto_shorts/common_filters/filter.ex` — 3 call sites
- `lib/ecto_shorts/common_filters/join.ex` — 2 call sites
- `lib/ecto_shorts/common_filters/having.ex` — 1 call site
- `lib/ecto_shorts/testing.ex` — doctest

### Design documents

- `guides/DESIGN.md` — internal shape design with examples and rules
- `guides/RULES.md` — user-facing filter ordering rules (to be updated)
- `guides/WORKED_EXAMPLES.md` — user-facing examples (to be updated)

## Acceptance criteria (Phase 3)

- `mix test --seed 0` passes with 0 failures after test inputs updated to new shapes
- C2 clause deleted from `dynamics.ex`
- `dynamics.ex` is a thin entrypoint only (no `append_predicates`, no `merge_dynamic`)
- Postgres adapter handles boolean combining via `normalize_entries` + combiner reduce
- No tuple reordering anywhere in the codebase
- `{field, {bool_op, entries}}` shape never produced
- `guides/RULES.md` and `guides/WORKED_EXAMPLES.md` updated with new input shapes
- No new public API (callers unchanged)

---

## Exact code changes

Each change is numbered for independent approval.

### Change 1: `lib/ecto_shorts/dynamics.ex` — strip to thin entrypoint

Delete everything from line 65 (`alias Ecto.Query`) through line 222 (`defp source_has_schema?(_), do: false`). Keep only the module definition, moduledoc, `convert_to_dynamic/4`, and `adapter_for_repo!/1`.

**Delete lines 65-76 (aliases, requires, module attributes) and replace with:**
```elixir
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
```

**Delete lines 127-222 (all `append_predicates`, `build_dynamic`, `merge_dynamic`, `source_has_schema?`).**

**Replace `convert_to_dynamic/4` body (line 121-125) with:**
```elixir
  def convert_to_dynamic(source, binding_selector, term, opts \\ []) do
    adapter = adapter_for_repo!(opts)
    source = CommonSchema.normalize_source(source)
    adapter.convert_to_dynamic(source, binding_selector, term)
  end
```

### Change 2: `lib/ecto_shorts/dynamics/postgres.ex` — absorb `append_predicates` and boolean combining

**Add after `@datetime_operators` (line 287), before `@impl true` (line 289):**
```elixir
  @boolean_operators [:and, :or]
  @logger_prefix "EctoShorts.Dynamics"
```

**Add new public function after `operator?/1` (line 293) and before `build_dynamic/4` (line 295):**
```elixir
  @doc false
  def convert_to_dynamic(source, binding_selector, term) do
    append_predicates(source, nil, binding_selector, term)
  end

  defp append_predicates(source, dyn_left, binding_selector, {key, map})
       when is_map(map) and not is_struct(map) do
    append_predicates(source, dyn_left, binding_selector, {key, Map.to_list(map)})
  end

  defp append_predicates(source, dyn_left, binding_selector, {key, value}) do
    cond do
      key in @operators ->
        merge_predicate(dyn_left, :and, build_dynamic(source, binding_selector, key, value))

      key in @boolean_operators ->
        Enum.reduce(value, dyn_left, fn entry, dyn_acc ->
          dyn_right = append_predicates(source, nil, binding_selector, entry)
          merge_predicate(dyn_acc, key, dyn_right)
        end)

      source_has_schema?(source) ->
        schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

        if key not in schema_fields do
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
          )

          dyn_left
        else
          if Keyword.keyword?(value) do
            Enum.reduce(value, dyn_left, fn entry, dyn_acc ->
              append_predicates(source, dyn_acc, binding_selector, {key, entry})
            end)
          else
            merge_predicate(dyn_left, :and, build_dynamic(source, binding_selector, key, value))
          end
        end

      true ->
        if Keyword.keyword?(value) do
          Enum.reduce(value, dyn_left, fn entry, dyn_acc ->
            append_predicates(source, dyn_acc, binding_selector, {key, entry})
          end)
        else
          merge_predicate(dyn_left, :and, build_dynamic(source, binding_selector, key, value))
        end
    end
  end

  defp append_predicates(source, dyn_left, binding_selector, params) do
    if (is_map(params) and not is_struct(params)) or Keyword.keyword?(params) do
      Enum.reduce(params, dyn_left, fn {k, v}, dyn_acc ->
        append_predicates(source, dyn_acc, binding_selector, {k, v})
      end)
    else
      raise "Expected a map or keyword-list, got: #{inspect(params)}"
    end
  end

  defp merge_predicate(dyn_left, _op, nil), do: dyn_left
  defp merge_predicate(nil, _op, dyn_right), do: dyn_right
  defp merge_predicate(dyn_left, :and, dyn_right), do: Query.dynamic([], ^dyn_left and ^dyn_right)
  defp merge_predicate(dyn_left, :or, dyn_right), do: Query.dynamic([], ^dyn_left or ^dyn_right)

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false
```

**Change `build_dynamic/4` return handling — replace `reduce_entries` nil return with adapter warning:**

The existing `build_dynamic/4` stays as-is. The `merge_predicate` handles nil from `build_dynamic` (first clause `merge_predicate(dyn_left, _op, nil), do: dyn_left`).

Note: `opts` is not passed to `append_predicates` or `build_dynamic` since the adapter is already resolved by the time we're here.

### Change 3: `lib/ecto_shorts/dynamic.ex` — add `convert_to_dynamic` callback

**Add after the `build_dynamic` callback (line 376), before `end`:**
```elixir

  @callback convert_to_dynamic(
              source :: term(),
              binding_selector :: term(),
              term :: term()
            ) :: Ecto.Query.dynamic_expr() | nil
```

### Change 4: `guides/RULES.md` — update Rules 12/13

**Replace lines 137-151:**

**Before:**
```
**Rule 12:** When used at the field level, the `:and` operator must contain a list of comparison operators or other field-level expressions.

The `:and` operator combines multiple conditions with AND logic and must wrap a list of field-level filter entries.

Examples:
- `%{views: %{and: [>: 10, <: 20]}}` - `:and` wraps a list of operators
- `%{views: %{and: []}}` - `:and` can wrap an empty list

**Rule 13:** When used at the field level, the `:or` operator must contain a list of comparison operators or other field-level expressions.

The `:or` operator combines multiple conditions with OR logic and must wrap a list of field-level filter entries.

Examples:
- `%{published: %{or: [==: true, ==: false]}}` - `:or` wraps a list of operators
- `%{views: %{or: [>: 10, <: 5]}}` - `:or` wraps a list of operators
```

**After:**
```
**Rule 12:** The `:and` and `:or` operators are combiners that control how entries are merged.

When used at the field level, the combiner wraps a map or keyword list of field filters. The filters are flattened to individual `{field, {operator, value}}` entries and merged with the combiner.

Examples:
- `%{and: %{views: [>: 10, <: 20]}}` - `:and` combines `views > 10` AND `views < 20`
- `%{or: %{views: [>: 10, <: 5]}}` - `:or` combines `views > 10` OR `views < 5`
- `%{or: %{published: [==: true, ==: false]}}` - `:or` combines `published = true` OR `published = false`
```

### Change 5: `guides/WORKED_EXAMPLES.md` — update field-level boolean examples

**Replace lines 231-239:**

**Before:**
```
## Logical Operators - Field Level Examples

%{views: %{and: [>: 10, <: 20]}}

%{views: %{and: []}}

%{published: %{and: [==: true, !=: false]}}

%{published: %{or: [==: true, ==: false]}}
```

**After:**
```
## Logical Operators - Field Level Examples

%{and: %{views: [>: 10, <: 20]}}

%{and: %{views: []}}

%{and: %{published: [==: true, !=: false]}}

%{or: %{published: [==: true, ==: false]}}

%{or: %{views: [>: 10, <: 5]}}
```

**Also update line 247:**

**Before:** `%{or: [[published: %{or: [==: true, ==: false]}], [published: %{or: [==: true, ==: false]}]]}`
**After:** `%{or: [%{or: %{published: [==: true, ==: false]}}, %{or: %{published: [==: true, ==: false]}}]}`

**Also update line 287:**

**Before:** `[or_where: %{views: %{or: [>: 10, <: 5]}}, published: true]`
**After:** `[or_where: %{or: %{views: [>: 10, <: 5]}}, published: true]`

### Change 6: Tests — `binding_and_boolean_test.exs` input shape updates

**Line 16:** `%{views: %{and: [>: 10, <: 20]}}` → `%{and: %{views: [>: 10, <: 20]}}`
**Line 23:** `%{views: %{and: []}}` → `%{and: %{views: []}}`
**Line 33:** `%{published: %{and: [==: true, !=: false]}}` → `%{and: %{published: [==: true, !=: false]}}`
**Line 44:** `%{views: %{or: [>: 10, <: 5]}}` → `%{or: %{views: [>: 10, <: 5]}}`
**Line 55:** `%{published: %{or: [==: true, ==: false]}}` → `%{or: %{published: [==: true, ==: false]}}`
**Line 107:** `%{or: [[published: [or: [==: true, ==: false]]], [published: [or: [==: true, ==: false]]]]}` → `%{or: [%{or: %{published: [==: true, ==: false]}}, %{or: %{published: [==: true, ==: false]}}]}`
**Line 184:** `%{id: %{or: [[published: true, does_not_exist: "value"]]}}` → update to new shape and update warning assertion
**Line 463:** `[or_where: %{views: %{or: [>: 10, <: 5]}}, published: true]` → `[or_where: %{or: %{views: [>: 10, <: 5]}}, published: true]`

### Change 7: Tests — `crud_test.exs` input shape updates

**Line 954:** `%{views: %{and: [>: 10, <: 20]}}` → `%{and: %{views: [>: 10, <: 20]}}`
**Line 967:** `%{views: %{or: [<: 5, >: 10]}}` → `%{or: %{views: [<: 5, >: 10]}}`
**Line 991:** `or_where: %{views: %{or: [<: 5, >: 10]}}` → `or_where: %{or: %{views: [<: 5, >: 10]}}`

### Change 8: Tests — `query_operation_test.exs` input shape updates

**Line 1763:** `%{where: [published: true, views: %{or: [>: 10, <: 5]}]}` → `%{where: [published: true, or: %{views: [>: 10, <: 5]}]}`

### Change 9: Tests — `dynamics_test.exs` — no input shape changes needed

Top-level boolean tests (`%{or: [published: true, published: false]}`, `%{and: [published: true, title: "hi"]}`) already use the correct shape (bool_op outermost). No changes needed.

---

## Status

Design decisions complete. Ready for implementation in next session.
