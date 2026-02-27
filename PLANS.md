## Tasks

- [ ] Pre-flight for every task: run `git status`, validate uncommitted changes are still intentional, and ask for clarification before editing if anything unexpected appears
- [x] Add support for `lock/3`
- [ ] Add support for `windows/3`
- [ ] Add support for `with_cte/3`
- [ ] Add support for `recursive_ctes/2`
- [ ] Add support for `with_named_binding/3`
- [ ] Add support for `with_ties/3`
- [ ] Add support for `put_query_prefix/2`
- [ ] Add support for `dynamic/2` as a first-class filter op payload
- [x] Add support for `all/1` helper expression
- [ ] Add support for `any/1` helper expression
- [ ] Add support for `exists/1` helper expression
- [ ] Add support for arithmetic helper expressions: `+`, `-`, `*`, `/`
- [ ] Add support for date/time helpers: `datetime_add/3`, `date_add/3`, `from_now/2`, `ago/2`
- [ ] Add support for expression helpers: `fragment/1`, `type/2`, `as/1`, `parent_as/1`
- [ ] Add support for alias helpers: `selected_as/1`, `selected_as/2`
- [ ] Add support for aggregate helpers: `count(field, :distinct)` and `filter/2`
- [ ] Add support for statistical helpers: `stddev/1`, `stddev_pop/1`, `stddev_samp/1`
- [ ] Add support for window helpers: `over/2`, `first_value/1`, `last_value/1`, `lag/3`, `lead/3`, `nth_value/2`
- [ ] Add support for value/fragments helpers: `coalesce/2`, `constant/1`, `identifier/1`, `splice/1`, `values/2`, `json_extract_path/2`
- [ ] Add support for expression helper `merge/2`
- [ ] Add support for symbolic positional binding selectors: `{:at, :first}` and `{:at, :last}` (`:first` should behave like `{:as, nil}`)
- [ ] Ensure filters in the same payload as canonical `:join` default to the newly joined (last) binding, unless an explicit `:bind` selector overrides it

## Plans

### First Task

`lock/3`

Reason: Lowest complexity and lowest risk. `lock/3` is compile-time constrained in Ecto, so this task introduces delegated lock builders plus the expression resolver adapter abstraction used for compile-time-only expression features.

### 1) `lock/3`

Input:

```elixir
%{lock: fn query -> from(p in query, lock: "FOR UPDATE") end}
```

Behavior:
- Add `:lock` to `@query_filters` in `EctoShorts.CommonFilters`.
- Route `:lock` through `EctoShorts.CommonFilters.Filter` (same pattern as other non-binding query-level filters).
- In `Filter.apply_pagination_expr/6`, add `:lock` support that applies a user-provided unary query-builder function.
- Support resolver payloads (`%{name: ..., values: ...}`) that resolve through `CommonFilters.QuerySourceProvider`.
- Add a `CommonFilters` expression-resolver abstraction with a default adapter module under `common_filters`.
- Refactor `join.ex` to use the expression-resolver abstraction.

Output:
- Query contains lock clause, built by end-user lock function/resolver.

Tests:
- Lock via direct query-builder function.
- Lock via resolver payload with runtime `:query_source_provider`.
- Existing join resolver tests still pass after abstraction refactor.

### 2) `windows/3`

Input:

```elixir
%{windows: [w: [partition_by: :author_id, order_by: [desc: :inserted_at]]]}
```

Behavior:
- Add dedicated windows module in `common_filters`.
- Parse map/keyword window definitions and pass to `Ecto.Query.windows/3`.

Output:
- Query with named windows available for `over/2`.

Tests:
- Single and multi-window definitions.
- Field references with binding selectors.

### 3) `with_cte/3`

Input:

```elixir
%{with_cte: [post_subset: [as: ^subquery]]}
```

Behavior:
- Add `:with_cte` to `@query_filters` in `EctoShorts.CommonFilters`.
- Route `:with_cte` in `apply_query_builder/6` to a dedicated `EctoShorts.CommonFilters.WithCte` module.
- `WithCte.build/6` accepts:
  - keyword list payload
  - map payload
  - single entry tuple `{cte_name, cte_opts}`
- Per-entry options:
  - `:as` (required)
  - `:materialized` (optional boolean)
  - `:operation` (optional `:all | :update_all | :delete_all`)
- CTE name normalization:
  - atom names are converted to string names
  - binary names are used as-is
  - invalid names are warned and skipped
- `:as` normalization:
  - `%Ecto.Query{}` and `%Ecto.SubQuery{}` are accepted directly
  - `%{from: ..., query: ...}` or keyword equivalent is converted with `CommonFilters.convert_params_to_filter/3`
  - invalid `:as` values are warned and skipped
- Composition uses Ecto public API:
  - `Ecto.Query.with_cte(query, ^cte_name, as: ^cte_query, ...)`

Output:
- Query includes named CTE(s).

Tests:
- Supports keyword payload with query `:as`.
- Supports map payload with query `:as`.
- Supports multiple CTE entries in a single payload.
- Supports optional `:materialized` and `:operation`.
- Supports `:as` as nested filter payload (`%{from: ..., query: ...}` and keyword equivalent).
- Invalid payload cases (`missing :as`, invalid cte name type, invalid definition type) warn and no-op.

#### `with_cte/3` Spec Matrix

`@spec convert_params_to_filter(module() | Ecto.Queryable.t(), map() | keyword(), keyword()) :: Ecto.Query.t()`

Example A (keyword payload):

```elixir
CommonFilters.convert_params_to_filter(
  Post,
  %{
    with_cte: [
      post_subset: [as: from(p in Post, where: p.published == ^true)]
    ]
  },
  []
)
```

Expected behavior:
- Builds and applies CTE `"post_subset"` with `as: ^query`.

Example B (map payload):

```elixir
CommonFilters.convert_params_to_filter(
  Post,
  %{
    with_cte: %{
      post_subset: %{as: from(p in Post, where: p.views > ^100)}
    }
  },
  []
)
```

Expected behavior:
- Map payload is normalized to keyword and produces the same CTE semantics.

Example C (operation/materialized):

```elixir
CommonFilters.convert_params_to_filter(
  {"update_posts", Post},
  %{
    with_cte: [
      update_posts: [
        as: from(p in Post, update: [set: [title: "Updated"]], select: p),
        operation: :update_all,
        materialized: false
      ]
    ]
  },
  []
)
```

Expected behavior:
- Applies data-modifying CTE with operation and materialization options.

Example D (`:as` via nested filter payload):

```elixir
CommonFilters.convert_params_to_filter(
  Post,
  %{
    with_cte: [
      post_subset: [as: %{from: Post, query: %{published: true}}],
      popular_posts: [as: %{from: Post, query: %{views: %{>: 1000}}}]
    ]
  },
  []
)
```

Expected behavior:
- Nested filter payload is converted to query first, then applied to each CTE entry.

Example E (invalid missing `:as`):

```elixir
CommonFilters.convert_params_to_filter(
  Post,
  %{
    with_cte: [
      post_subset: [operation: :all]
    ]
  },
  []
)
```

Expected behavior:
- Warns about missing `:as` and leaves the query unchanged for that entry.

### 4) `recursive_ctes/2`

Input:

```elixir
%{recursive_ctes: true}
```

Behavior:
- Route to `Ecto.Query.recursive_ctes/2`.

Output:
- Query toggles recursive CTE mode.

Tests:
- `true` and `false` forms.
- Composition with `with_cte`.

### Approved `:bind` Example Patches (Must Match Implementation Exactly)

#### Example 1: `lib/ecto_shorts/common_filters.ex`

```elixir
# replace
@boolean_operators [:as, :at]

# with
@binding_selector_key :bind
@binding_selector_modes [:as, :at]
```

```elixir
# in reduce_filter_params/6 cond, replace this branch:
# key in @boolean_operators -> ...

# with:
key == @binding_selector_key ->
  reduce_bind_params(
    schema_source,
    query,
    binding_selector,
    filter_op,
    value,
    opts
  )
```

```elixir
# add these new private functions (exact):

defp reduce_bind_params(
       schema_source,
       query,
       binding_selector,
       filter_op,
       bind_params,
       opts
     )
     when is_map(bind_params) and not is_struct(bind_params) do
  reduce_bind_params(
    schema_source,
    query,
    binding_selector,
    filter_op,
    Map.to_list(bind_params),
    opts
  )
end

defp reduce_bind_params(
       schema_source,
       query,
       binding_selector,
       filter_op,
       bind_params,
       opts
     )
     when is_list(bind_params) do
  if Keyword.keyword?(bind_params) do
    Enum.reduce(bind_params, query, fn {binding_mode, scoped_params}, query_acc ->
      if binding_mode in @binding_selector_modes do
        scoped_params =
          cond do
            is_map(scoped_params) and not is_struct(scoped_params) ->
              Map.to_list(scoped_params)

            Keyword.keyword?(scoped_params) ->
              scoped_params

            true ->
              raise ArgumentError,
                    "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(scoped_params)}"
          end

        Enum.reduce(scoped_params, query_acc, fn {binding_target, params}, q2 ->
          reduce_binding_params(
            schema_source,
            q2,
            {binding_mode, binding_target},
            filter_op,
            params,
            opts
          )
        end)
      else
        raise ArgumentError,
              "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
      end
    end)
  else
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
  end
end

defp reduce_bind_params(
       _schema_source,
       _query,
       _binding_selector,
       _filter_op,
       bind_params,
       _opts
     ) do
  raise ArgumentError,
        "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
end
```

```elixir
# in reduce_binding_params/6, replace the invalid selector branch from warning to raise:

binding_selector ->
  raise ArgumentError,
        "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: #{inspect(binding_selector)}"
```

#### Example 2: `lib/ecto_shorts/common_filters/order_by.ex`

```elixir
# replace
@boolean_operators [:as, :at]

# with
@binding_selector_key :bind
@binding_selector_modes [:as, :at]
```

```elixir
# replace reduce_order_by/4 tuple handling with this exact version:

defp reduce_order_by(filter_op, query, binding_selector, {key, params})
     when is_map(params) and not is_struct(params) do
  reduce_order_by(filter_op, query, binding_selector, {key, Map.to_list(params)})
end

defp reduce_order_by(filter_op, query, binding_selector, {@binding_selector_key, bind_params}) do
  reduce_order_by_bind(filter_op, query, binding_selector, bind_params)
end

defp reduce_order_by(filter_op, query, binding_selector, {key, value}) do
  reduce_order_by_expr(filter_op, query, binding_selector, {key, value})
end
```

```elixir
# replace reduce_order_by/4 list handling with this exact version:

defp reduce_order_by(filter_op, query, binding_selector, entries) when is_list(entries) do
  if Keyword.keyword?(entries) do
    case Enum.split_with(entries, fn {k, _} -> k == @binding_selector_key end) do
      {[], order_entries} ->
        reduce_order_by_expr(filter_op, query, binding_selector, order_entries)

      {bind_entries, []} ->
        Enum.reduce(bind_entries, query, fn entry, query_acc ->
          reduce_order_by(filter_op, query_acc, binding_selector, entry)
        end)

      {bind_entries, order_entries} ->
        query_with_order =
          reduce_order_by_expr(filter_op, query, binding_selector, order_entries)

        Enum.reduce(bind_entries, query_with_order, fn entry, query_acc ->
          reduce_order_by(filter_op, query_acc, binding_selector, entry)
        end)
    end
  else
    reduce_order_by_expr(filter_op, query, binding_selector, entries)
  end
end
```

```elixir
# add these new helpers exactly:

defp reduce_order_by_bind(filter_op, query, binding_selector, bind_params)
     when is_map(bind_params) and not is_struct(bind_params) do
  reduce_order_by_bind(filter_op, query, binding_selector, Map.to_list(bind_params))
end

defp reduce_order_by_bind(filter_op, query, binding_selector, bind_params) when is_list(bind_params) do
  if Keyword.keyword?(bind_params) do
    Enum.reduce(bind_params, query, fn {binding_mode, scoped_params}, query_acc ->
      if binding_mode in @binding_selector_modes do
        scoped_params =
          cond do
            is_map(scoped_params) and not is_struct(scoped_params) ->
              Map.to_list(scoped_params)

            Keyword.keyword?(scoped_params) ->
              scoped_params

            true ->
              raise ArgumentError,
                    "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(scoped_params)}"
          end

        Enum.reduce(scoped_params, query_acc, fn {binding_target, next_value}, q ->
          reduce_order_by(filter_op, q, {binding_mode, binding_target}, next_value)
        end)
      else
        raise ArgumentError,
              "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
      end
    end)
  else
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
  end
end

defp reduce_order_by_bind(_filter_op, _query, _binding_selector, bind_params) do
  raise ArgumentError,
        "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
end
```

### 5) `with_named_binding/3`

Input:

```elixir
%{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]]}}
```

Behavior:
- Ensure named binding exists; if missing, apply provided binding builder payload.

Output:
- Query always has target named binding.

Tests:
- No-op when binding exists.
- Binding creation when missing.

### 6) `with_ties/3`

Input:

```elixir
%{with_ties: true}
```

Behavior:
- Route to `Ecto.Query.with_ties/3`.

Output:
- Query includes WITH TIES behavior for limiting.

Tests:
- With limit/order_by combination.
- No-op/behavior when missing order context (as Ecto permits).

### 7) `put_query_prefix/2`

Input:

```elixir
%{put_query_prefix: "tenant_a"}
```

Behavior:
- Route to `Ecto.Query.put_query_prefix/2`.

Output:
- Query source prefix updated.

Tests:
- Schema and query sources.
- Prefix override behavior.

### 8) `dynamic/2` as first-class filter op payload

Input:

```elixir
%{dynamic: dynamic([p], p.views > ^10)}
```

Behavior:
- Add explicit op handling for direct dynamic payload injection into where/having pipelines.

Output:
- Dynamic expression applied without re-parsing DSL.

Tests:
- `where`, `having`, `or_having` integration.

### 9) `all/1` helper expression

Input:

```elixir
%{views: %{all: %{>: subquery_expr}}}
```

Behavior:
- Extend dynamic helper parsing to support `all/1` value wrappers.

Output:
- Expression compiles using `all(...)`.

Tests:
- Comparison operators with wrapped subquery.

### 10) `any/1` helper expression

Input:

```elixir
%{views: %{any: %{>: subquery_expr}}}
```

Behavior:
- Extend dynamic helper parsing for `any(...)`.

Output:
- Expression compiles using `any(...)`.

Tests:
- Comparison operators with wrapped subquery.

### 11) `exists/1` helper expression

Input:

```elixir
%{exists: subquery_expr}
```

Behavior:
- Add top-level helper op that compiles to `exists(...)`.

Output:
- Dynamic predicate includes `exists`.

Tests:
- Exists in where/having contexts.

### 12) Arithmetic helpers `+`, `-`, `*`, `/`

Input:

```elixir
%{views: %{>: {:+, [:views, 10]}}}
```

Behavior:
- Add normalized arithmetic expression AST in dynamic compiler.

Output:
- Arithmetic expression in predicate.

Tests:
- Each arithmetic operator and nested expression cases.

### 13) Date/time helpers

Input:

```elixir
%{inserted_at: %{>: {:ago, [1, "day"]}}}
```

Behavior:
- Add helper expression handling for `datetime_add/date_add/from_now/ago`.

Output:
- Temporal expressions compile in dynamic predicate context.

Tests:
- Each helper with `==`, `>`, `<` style comparisons.

### 14) `fragment/1`, `type/2`, `as/1`, `parent_as/1`

Input:

```elixir
%{field: %{==: {:type, [:inserted_at, :utc_datetime]}}}
```

Behavior:
- Introduce controlled DSL shapes for advanced expression helpers.

Output:
- Valid advanced expressions supported in dynamic pipeline.

Tests:
- Fragment/type casting/binding alias resolution.

### 15) `selected_as/1`, `selected_as/2`

Input:

```elixir
%{selected_as: [metric: :avg_views]}
```

Behavior:
- Add select alias helper support and downstream references.

Output:
- Aliased selected expressions reusable in query clauses.

Tests:
- Select + order_by/having alias references.

### 16) `count(field, :distinct)` and `filter/2`

Input:

```elixir
%{count: %{id: {:distinct, %{>: 1}}}}
```

Behavior:
- Extend aggregate helper parsing to support distinct and filter wrappers.

Output:
- Distinct/filtered aggregates in dynamic expressions.

Tests:
- Distinct count and filtered aggregate SQL assertions.

### 17) Statistical helpers

Input:

```elixir
%{views: %{stddev: %{>: 1.0}}}
```

Behavior:
- Add helper expressions for `stddev`, `stddev_pop`, `stddev_samp`.

Output:
- Statistical predicate expressions compile.

Tests:
- All three helpers with comparison operators.

### 18) Window helpers

Input:

```elixir
%{views: %{over: [avg: [partition_by: :author_id], >: 10]}}
```

Behavior:
- Add `over/2` and window function helpers plus integration with `windows/3`.

Output:
- Windowed expressions compile and can be filtered/selected.

Tests:
- `over`, `first_value`, `last_value`, `lag`, `lead`, `nth_value`.

### 19) Value/fragments helpers

Input:

```elixir
%{field: %{==: {:coalesce, [:title, "untitled"]}}}
```

Behavior:
- Add helper forms for `coalesce`, `constant`, `identifier`, `splice`, `values`, `json_extract_path`.

Output:
- Value/fragments helpers compile through dynamic adapter.

Tests:
- One test per helper shape and validation for malformed payloads.

### 20) `merge/2` expression helper

Input:

```elixir
%{select: %{merge: [%{id: :id}, %{title: :title}]}}
```

Behavior:
- Add expression-level merge support where Ecto allows it.

Output:
- Merged map expressions in select pipeline.

Tests:
- Merge composition with `select` / `select_merge`.

---

## Refactor Task: `EctoShorts.Dynamics` as a Dumb Router

### Why this refactor

Current complexity is coming from `EctoShorts.Dynamics` interpreting expression semantics (especially `:not`) that should be owned by adapter/spec expression modules.  
Goal: keep `dynamics.ex` focused on routing + normalization + dynamic merging only.

### Rules and constraints (must be enforced)

1. Do **not** edit files without explicit approval.
2. For the refactor implementation, only code file to touch is `lib/ecto_shorts/dynamics.ex`.
3. `append_schema_predicate` and `append_field_predicate` cannot both exist.
4. Final `lib/ecto_shorts/dynamics.ex` must be `<= 200` lines.
5. Do not use `Enum.all?/2`.
6. Do not do full-collection “helper-only” checks like:
   - `Enum.reduce(value, true, ...)` for map-wide validation decisions.
7. Do not use callback-function shaping style at call sites:
   - `append_predicate_items(..., fn item -> ... end)`.
8. Do not inline nested combinators like:
   - `merge_dynamic(dyn_a, :and, merge_boolean_predicates(...))`.
9. Compile before presenting code as final.
10. Treat design as the fix (not patching symptoms): `:not` semantics should be interpreted in expression modules/specs, not in `dynamics.ex`.

### Data model alignment (full enum we are modeling)

`dynamics.ex` should model payload transport, not expression meaning.

```elixir
@type params ::
        %{optional(any()) => params()}
        | keyword(params())
        | {key(), params()}
        | scalar()

@type key :: atom() | String.t()
@type scalar :: term()

# expression payloads passed through to adapter/specs
@type expr ::
        scalar()
        | {atom(), scalar() | expr() | [expr()] | keyword(expr())}
        | keyword(expr())
        | [expr()]
        | %{optional(any()) => expr()}
```

Operational enum through `append_predicates/5`:

1. **Top-level map** -> `Map.to_list/1` -> reduce entries.
2. **Top-level list** -> reduce entries.
3. **Top-level tuple `{key, value}`**:
   - if `key in @boolean_operators` -> boolean merge path.
   - else if schema-aware + key is a schema field -> route to field append path.
   - else -> route directly to adapter expression build path.
4. **Field append path**:
   - normalize expression payloads only (`normalize_expression_params/1`),
   - pass normalized items to adapter/spec unchanged in meaning.

### Target architecture (high-level)

- Keep only one predicate append function for non-boolean field routing.
- Remove local semantic interpretation for `:not`, `:any`, `:all`, etc. from `dynamics.ex`.
- Continue helper payload conversion (`apply_helper_expressions/4`) where needed for query-builder payloads (`[source: ..., query: ...]`), but avoid semantic branching explosion.
- Adapter/spec modules remain the owner of operator semantics.

### Refactor phases

#### Phase 1 — Flatten control flow

- Collapse branching in `append_predicates/5` to:
  - boolean branch
  - collection normalization (map/list)
  - generic tuple routing

Snippet target:

```elixir
defp append_predicates(source, dyn_a, binding_selector, {key, value}, opts) do
  # resolve adapter + schema routing
  # no operator semantic decisions here
end
```

#### Phase 2 — Make field append dumb

- Replace multi-clause semantic `append_field_predicate` logic with transport-oriented shaping only.
- No dedicated `:not` handling trees in `dynamics.ex`.

Snippet target:

```elixir
defp append_field_predicate(source, dyn_a, binding_selector, key, value, opts) do
  append_predicate_items(source, dyn_a, binding_selector, key, value, opts, :identity)
end
```

#### Phase 3 — Replace callback shaping API

- Remove `fn item -> ...` callback strategy in `append_predicate_items`.
- Use explicit mode tagging + pure shape function.

Snippet target:

```elixir
append_predicate_items(..., :identity)
append_predicate_items(..., {:op, op})
```

```elixir
defp shape_item(:identity, item), do: item
defp shape_item({:op, op}, item), do: {op, item}
```

#### Phase 4 — Enforce constraints and validate

- Ensure no `Enum.all?/2`.
- Ensure no full-collection helper-only scan gate.
- Ensure no nested inline merge combinator calls.
- Ensure line count `<= 200`.
- Compile + run focused suites:
  - `mix test test/ecto_shorts/dynamics_test.exs`
  - `mix test test/ecto_shorts/common_filters_test.exs`

### Acceptance checklist

- [ ] `lib/ecto_shorts/dynamics.ex` is `<= 200` lines
- [ ] `append_schema_predicate*` removed (or absent), and no dual-ownership with `append_field_predicate`
- [ ] no `Enum.all?/2`
- [ ] no callback-based shaping at `append_predicate_items` call sites
- [ ] no `merge_dynamic(..., merge_boolean_predicates(...))` nesting
- [ ] `dynamics.ex` is transport/router oriented, not semantic interpreter for `:not`
- [ ] compiles and focused tests pass
