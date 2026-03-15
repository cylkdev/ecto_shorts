defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Converts public filter params into an `Ecto.Query`.

  `EctoShorts.CommonFilters` is the caller-facing query language for the
  library. Ordinary callers usually enter through `convert_params_to_filter/3`.
  Advanced integrations can also use `build_query/6`, which is the public
  query-builder callback boundary used by the dispatcher.

  The module accepts a source plus a map or keyword list of params, sorts the
  entries into a predictable evaluation order, and routes each key to the
  appropriate builder.

  ## Accepted sources and param containers

  `convert_params_to_filter/3` accepts:

  * a schema module
  * an `{source, schema}` tuple
  * a prebuilt `Ecto.Query`

  Params can be either a map or a keyword list.

  Use a **keyword list** when duplicate keys matter, for example when you want
  multiple `where:`, `or_where:`, `join:`, or `with_cte:` entries and need to
  preserve their order.

  ## Binding selectors

  Two top-level shapes retarget the subsequent filters to a specific binding:

  * `:as` selects a named binding already present in the query.

        %{as: %{author: %{select: :first_name}}}

  * `:at` selects a positional binding. Positions are 1-based, and `:first`
    and `:last` are accepted aliases inside the `at:` map.

        %{at: %{2 => %{select: :first_name}}}
        %{at: %{first: %{select: :title}}}
        %{at: %{last: %{select: :body}}}

  These selectors are first-class public shapes. They are not wrapped in a
  separate `:bind` layer.

  ## Boolean and predicate groups

  The top-level boolean and predicate keys are transparent grouping operators:

  * `:where` / `:or_where` add explicit predicate clauses
  * `:and` expands its contents as `WHERE` conditions
  * `:or` expands its contents as `OR WHERE` conditions

  Supported value shapes include maps, keyword lists, and lists of maps or
  keyword lists.

  ## Evaluation order

  When params are provided as a keyword list, entries are reordered before
  evaluation:

  1. `:where`
  2. all other filters
  3. `:or_where`
  4. terminal filters such as `:last` and `:subquery`

  This keeps ordinary predicates ahead of `OR WHERE` clauses and makes the
  generated query shape more predictable.

  ## Major supported filter families

  The live public filter language includes all of the following families:

  * field equality and comparison operators
  * aggregate operators and aggregate nil checks
  * arithmetic expressions
  * string matching and string transformations
  * negation wrappers
  * date and datetime wrappers
  * join construction and association shorthand
  * ordering, grouping, having, distinct, limits, offsets, and first/last
  * projection through `:select` and `:select_merge`
  * eager loading through `:preload`
  * query mutation via `:update`
  * exclusion and query-prefix helpers
  * subqueries, set operations, recursive CTEs, `with_cte`, windows, and
    `with_ties`
  * named-binding support and explicit binding creation with
    `:with_named_binding`

  The module docs focus on caller-visible language. They do not attempt to
  restate every internal builder implementation detail.

  ## Filter key reference

  Keys not listed below, and not recognized as schema associations, are treated
  as direct field filters.

  | Key | Group | Description |
  | --- | --- | --- |
  | `:where` | predicate | add an AND `WHERE` clause |
  | `:or_where` | predicate | add an `OR WHERE` clause |
  | `:and` | boolean group | expand contents as AND predicates |
  | `:or` | boolean group | expand contents as OR predicates |
  | `:as` | binding selector | target a named binding |
  | `:at` | binding selector | target a positional binding (`1`, `2`, `:first`, `:last`) |
  | `:join` | join | add joins for associations, schemas, tables, queries, subqueries, or provider-backed fragments |
  | `:order_by` / `:prepend_order_by` / `:reverse_order` | sorting | control ordering |
  | `:group_by` / `:having` / `:or_having` | grouping | aggregate grouping and post-aggregate predicates |
  | `:distinct` | uniqueness | apply `DISTINCT` |
  | `:limit` / `:offset` / `:first` / `:last` | cardinality | control result count and position |
  | `:select` / `:select_merge` | projection | control the selected shape |
  | `:preload` | eager load | preload associations |
  | `:subquery` | nested query | wrap the current query in a subquery |
  | `:lock` | concurrency | apply a lock by `%{name: atom}` or `[name: atom]`, with optional provider-backed `values:` |
  | `:exclude` | removal | remove a query clause such as `:order_by` |
  | `:update` | mutation | build `update_all` update expressions |
  | `:put_query_prefix` | namespace | set the query prefix |
  | `:recursive_ctes` / `:with_cte` | CTE | enable recursive CTEs and add `WITH` entries |
  | `:windows` / `:with_ties` | windowing | configure windows and tie-handling |
  | `:with_named_binding` | binding | ensure a named binding exists before subsequent filters |
  | `:union`, `:union_all`, `:except`, `:except_all`, `:intersect`, `:intersect_all` | set composition | combine the current query with another query |

  ## Join forms and association shorthand

  `:join` supports multiple public outer-key forms:

  * explicit source-family keys such as `association:`, `schema:`, `table:`,
    `query:`, `subquery:`, and `fragment:`
  * an explicit `type:` source-family selector
  * association shorthand, where the outer key is the association name itself

  In explicit join payloads, `type:` selects the source family and
  `qualifier:` selects the join mode such as `:left` or `:inner`.

  Any key that matches a declared association on the schema is also treated as
  association shorthand:

      %{comments: %{approved: true}}

  That shape ensures the association binding exists and then applies the nested
  filters to that binding.
  """

  alias EctoShorts.CommonQuery
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.CommonFilters.API

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_operator [:as, :at]

  @behaviour EctoShorts.Adapter.QueryBuilder

  @doc """
  Builds an `Ecto.Query` from `source` by applying each entry in `params`.

  ## Arguments

    * `source` - a schema module, `{source, schema}` tuple, or an
      existing `Ecto.Query`.
    * `params` - a map or keyword list of filter params. Keyword lists
      preserve duplicate keys (e.g. multiple `where:` entries), which is
      required for composing several independent `WHERE` or `OR WHERE`
      clauses. See the module doc for the full key reference.
    * `opts` - keyword list of options.

  ## Options

    * `:sorter` - a unary function that receives the normalised keyword
      list and returns a reordered keyword list. Defaults to the built-in
      sort that places `:where` first, then other filters, then
      `:or_where`, then terminal filters (`:last`, `:subquery`).
    * `:query_builder` - a module that implements
      `EctoShorts.Adapter.QueryBuilder`. When set, all `build_query/6`
      calls are delegated to that module instead of the default
      `EctoShorts.CommonFilters.API`.
    * `:query_provider` - provider module used by filter families that
      resolve callback-driven query fragments such as provider-backed joins
      and locks.

  ## Examples

      iex> CommonFilters.convert_params_to_filter(Post, %{published: true}, [])
      #Ecto.Query<from p0 in Post, where: p0.published == ^true>

      iex> CommonFilters.convert_params_to_filter(Post, %{and: %{views: 5, published: true}}, [])
      #Ecto.Query<from p0 in Post, where: p0.published == ^true, where: p0.views == ^5>

      iex> CommonFilters.convert_params_to_filter(
      ...>   Post,
      ...>   [where: %{published: true}, or_where: %{title: "Draft"}],
      ...>   []
      ...> )
      #Ecto.Query<from p0 in Post, where: p0.published == ^true, or_where: p0.title == ^"Draft">

      iex> CommonFilters.convert_params_to_filter(
      ...>   source,
      ...>   %{as: %{author: %{select: :first_name}}},
      ...>   []
      ...> )
      #Ecto.Query<...>
  """
  def convert_params_to_filter(source, params, opts) do
    query = CommonSchema.to_query(source)
    sorter = opts[:sorter] || (&sort_filter_params/1)

    params
    |> to_keyword()
    |> sorter.()
    |> Enum.reduce(query, fn {key, value}, query_acc ->
      apply_filters(:where, source, query_acc, {:as, nil}, {key, value}, opts)
    end)
  end

  defp apply_filters(filter, source, query, selected_binding, {key, term}, opts) do
    cond do
      key in @binding_operator ->
        Enum.reduce(term, query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            filter,
            source,
            query_acc,
            resolve_binding_selector(query_acc, key, inner_key),
            inner_value,
            opts
          )
        end)

      key in API.filter_group(:predicate) ->
        reduce_filter_group_or_build(key, source, query, selected_binding, term, opts)

      key in API.filter_group(:post_aggregate) ->
        reduce_filter_group_or_build(key, source, query, selected_binding, term, opts)

      association_key?(source, key) ->
        if container?(term) do
          query
          |> ensure_association_binding(source, key, opts)
          |> reduce_association_filters(filter, source, key, term, opts)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected association filter value to be a map or keyword list, got: #{inspect(term)}"
          )

          query
        end

      key in API.filters() ->
        build_query(key, source, query, selected_binding, term, opts)

      key == :and ->
        if filter_group_list?(term) do
          Enum.reduce(term, query, fn entry, query_acc ->
            apply_filters(filter, source, query_acc, selected_binding, to_keyword(entry), opts)
          end)
        else
          Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
            apply_filters(filter, source, query_acc, selected_binding, {inner_key, inner_value}, opts)
          end)
        end

      key == :or ->
        if filter_group_list?(term) do
          Enum.reduce(term, query, fn entry, query_acc ->
            apply_filters(:or_where, source, query_acc, selected_binding, to_keyword(entry), opts)
          end)
        else
          Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
            or_entries(source, query_acc, selected_binding, inner_key, inner_value, opts)
          end)
        end

      true ->
        build_query(filter, source, query, selected_binding, {key, term}, opts)
    end
  end

  defp apply_filters(filter, source, query, selected_binding, term, opts) do
    Enum.reduce(term, query, &apply_filters(filter, source, &2, selected_binding, &1, opts))
  end

  defp reduce_filter_group_or_build(filter, source, query, selected_binding, term, opts) do
    cond do
      filter_group_list?(term) ->
        Enum.reduce(term, query, fn entry, query_acc ->
          apply_filters(filter, source, query_acc, selected_binding, to_keyword(entry), opts)
        end)

      container?(term) ->
        Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
          apply_filters(
            filter,
            source,
            query_acc,
            selected_binding,
            {inner_key, inner_value},
            opts
          )
        end)

      true ->
        build_query(filter, source, query, selected_binding, term, opts)
    end
  end

  defp or_entries(source, query, selected_binding, key, value, opts) do
    build_query(:or_where, source, query, selected_binding, {key, value}, opts)
  end

  defp resolve_binding_selector(_query, :at, :first), do: {:at, 1}
  defp resolve_binding_selector(query, :at, :last), do: {:at, CommonQuery.query_binding_count(query)}
  defp resolve_binding_selector(_query, key, inner_key), do: {key, inner_key}

  defp reduce_association_filters(query, filter, source, key, term, opts) do
    Enum.reduce(to_keyword(term), query, fn {inner_key, inner_value}, query_acc ->
      apply_filters(filter, source, query_acc, {:as, key}, {inner_key, inner_value}, opts)
    end)
  end

  defp ensure_association_binding(query, source, key, opts) do
    build_query(
      :with_named_binding,
      source,
      query,
      {:as, nil},
      %{key => %{join: [association: [source: key, as: key]]}},
      opts
    )
  end

  defp to_keyword(map) when is_map(map) and not is_struct(map), do: map |> Map.to_list() |> to_keyword()
  defp to_keyword([]), do: []
  defp to_keyword([head | tail]), do: [to_keyword(head) | to_keyword(tail)]
  defp to_keyword({k, v}), do: {k, to_keyword(v)}
  defp to_keyword(term), do: term

  defp association_key?(source, key) do
    key in (CommonSchema.get_schema_reflection(source, :associations) || [])
  end

  defp container?(term) do
    (is_map(term) and not is_struct(term)) or Keyword.keyword?(term)
  end

  defp filter_group_list?([]), do: true
  defp filter_group_list?([head | _]), do: container?(head)
  defp filter_group_list?(_), do: false

  @impl EctoShorts.Adapter.QueryBuilder
  @doc """
  Applies a single filter entry to the query.

  This is the `EctoShorts.Adapter.QueryBuilder` implementation for
  `EctoShorts.CommonFilters`. It dispatches `{filter, term}` to the
  appropriate internal builder module via `EctoShorts.CommonFilters.API`,
  or delegates to a custom `:query_builder` module when one is configured.

  ## Arguments

    * `filter` - the filter group atom (e.g. `:where`, `:order_by`, `:join`).
    * `source` - the queryable source.
    * `query` - the current `Ecto.Query.t()` being built.
    * `selected_binding` - the active binding selector: `{:as, atom()}` or
      `{:at, pos_integer()}`.
    * `term` - the filter value (a `{field, value}` tuple for field filters,
      or the raw value for structural filters like `:limit`).
    * `opts` - keyword options forwarded from the call site. When `:query_builder`
      is set, all dispatching is delegated to that module.

  Custom query builder implementations can call this function to fall
  through to the default dispatch after applying their own logic:

      defmodule MyApp.CustomQueryBuilder do
        @behaviour EctoShorts.Adapter.QueryBuilder

        @impl true
        def build_query(filter, source, query, selected_binding, term, opts) do
          EctoShorts.CommonFilters.build_query(filter, source, query, selected_binding, term, opts)
        end
      end

  If `opts[:query_builder]` points to a module that does not export
  `build_query/6`, the function logs a warning and returns the query
  unchanged. If `:query_builder` is present but is not a module, it raises
  `ArgumentError`.
  """
  def build_query(filter, source, query, selected_binding, term, opts) do
    case opts[:query_builder] || Config.query_builder() do
      nil ->
        API.build_query(filter, source, query, selected_binding, term, opts)

      module when is_atom(module) ->
        if function_exported?(module, :build_query, 6) do
          module.build_query(filter, source, query, selected_binding, term, opts)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Module does not export the required function build_query/6: #{inspect(module)}"
          )

          query
        end

      term ->
        raise ArgumentError, "Expect :query_builder option to a module, got: #{inspect(term)}"
    end
  end

  defp sort_filter_params(params) do
    where_filters = Enum.filter(params, fn {key, _val} -> key == :where end)
    or_where_filters = Enum.filter(params, fn {key, _val} -> key == :or_where end)
    terminal_filters = Enum.filter(params, fn {key, _val} -> key in [:last, :subquery] end)

    other_filters =
      Enum.filter(params, fn {key, _val} -> key not in [:where, :or_where, :last, :subquery] end)

    where_filters
    |> Kernel.++(other_filters)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
