defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Converts a map or keyword list of filter params into an `Ecto.Query`.

  `CommonFilters` is the central dispatch layer between the public filter
  params API and the individual query-builder modules. It normalises the
  input, sorts the entries into evaluation order, and routes each key to
  the appropriate builder.

  ## Top-level filter keys

  The following keys receive special routing treatment before the generic
  per-field dispatch:

  ### Binding selectors

  * `:as` - selects a named binding for the subsequent filter expressions.

        %{as: %{post: %{published: true}}}

  * `:at` - selects a positional binding (1-based integer, or the aliases
    `:first` / `:last`) for the subsequent filter expressions.

        %{at: %{1 => %{published: true}}}
        %{at: %{first: %{published: true}}}

  ### Boolean group operators

  These are transparency wrappers; they do not add a new nesting level -
  they expand their contents into individual `WHERE` or `OR WHERE` clauses
  on the query directly.

  * `:and` - applies each condition inside the value as a `WHERE` clause
    (AND-joined with any existing clauses). Accepts a map, keyword list,
    or list of maps / keyword lists.

        %{and: %{views: 5, published: true}}
        %{and: [%{views: 5}, %{published: true}]}

  * `:or` - applies each condition inside the value as a separate
    `OR WHERE` clause. Accepts the same shapes as `:and`. A range value
    on a single field (`[>: 1, <: 10]`) AND-merges the range operators
    inside one `OR WHERE` expression.

        %{or: %{views: 5}}
        %{or: [%{views: 5}, %{published: true}]}

  ### Predicate filters

  * `:where` - adds a `WHERE` clause. Multiple `:where` entries in a
    keyword list are AND-joined in order.

        %{where: %{published: true}}
        [where: %{published: true}, where: %{views: 5}]

  * `:or_where` - adds an `OR WHERE` clause. Multiple `:or_where` entries
    in a keyword list are OR-joined in order.

        %{or_where: %{published: false}}
        [or_where: %{title: "A"}, or_where: %{title: "B"}]

  Both `:where` and `:or_where` also accept a list of maps or keyword
  lists; each element is applied as a separate clause.

        %{where: [%{published: true}, %{views: 5}]}
        %{or_where: [%{title: "A"}, %{title: "B"}]}

  ### Evaluation order

  When params are given as a keyword list, entries are reordered before
  evaluation:

  1. `:where` filters
  2. All other filters (field filters, `:and`, `:or`, joins, etc.)
  3. `:or_where` filters
  4. Terminal filters (`:last`, `:subquery`)

  This ensures plain field conditions always precede `OR WHERE` clauses,
  which is the most predictable SQL shape.

  ## Filter key reference

  Every key accepted by `convert_params_to_filter/3` is listed below.
  Keys that are not in this table (and are not schema associations) are
  treated as direct field equality filters (`WHERE field = value`).

  | Key | Group | Description |
  |---|---|---|
  | `:where` | predicate | AND `WHERE` clause |
  | `:or_where` | predicate | OR `WHERE` clause |
  | `:and` | boolean group | expand contents as AND `WHERE` clauses |
  | `:or` | boolean group | expand contents as `OR WHERE` clauses |
  | `:as` | binding selector | target a named binding for subsequent filters |
  | `:at` | binding selector | target a positional binding (1-based, or `:first`/`:last`) |
  | `:join` | association | add a join (named, anonymous, schema, table, fragment, subquery) |
  | `:order_by` | sorting | `ORDER BY` clause |
  | `:prepend_order_by` | sorting | prepend to existing `ORDER BY` |
  | `:reverse_order` | sorting | reverse the current `ORDER BY` |
  | `:group_by` | grouping | `GROUP BY` clause |
  | `:having` | post-aggregate | `HAVING` clause (post-aggregate filter) |
  | `:or_having` | post-aggregate | `OR HAVING` clause |
  | `:distinct` | uniqueness | `DISTINCT` clause |
  | `:limit` | cardinality | `LIMIT` clause |
  | `:first` | cardinality | alias for `limit: 1` |
  | `:last` | terminal result | reverse-order then `LIMIT 1` |
  | `:offset` | pagination | `OFFSET` clause |
  | `:select` | projection | `SELECT` clause |
  | `:select_merge` | projection | merge into an existing `SELECT` |
  | `:preload` | eager load | `Ecto.Query.preload/3` - atom, list, keyword list, or `{assoc, query}` tuple |
  | `:subquery` | nested query | wrap the current query in a subquery |
  | `:lock` | concurrency | `LOCK` clause - name atom, raw string, or unary function |
  | `:exclude` | removal | drop a clause from the query (e.g. `:order_by`) |
  | `:update` | mutation | `UPDATE SET` operations for `update_all` |
  | `:put_query_prefix` | namespace | set the query prefix (PostgreSQL schema) |
  | `:recursive_ctes` | recursive CTE | enable recursive CTE support |
  | `:with_cte` | CTE | add a CTE (`WITH` clause) |
  | `:windows` | window function | define window expressions |
  | `:with_ties` | tie handling | `FETCH FIRST … WITH TIES` |
  | `:with_named_binding` | binding | ensure a named binding exists |
  | `:union` / `:union_all` | set composition | `UNION` / `UNION ALL` |
  | `:except` / `:except_all` | set composition | `EXCEPT` / `EXCEPT ALL` |
  | `:intersect` / `:intersect_all` | set composition | `INTERSECT` / `INTERSECT ALL` |

  ## Association shorthand

  Any key that matches a declared association on the schema is
  automatically treated as a join filter. The value must be a map or
  keyword list of filter params to apply on the joined binding:

      %{comments: %{approved: true}}

  This is equivalent to using `:join` explicitly with the association name.
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

      association_filter?(source, key, term) ->
        query
        |> ensure_association_binding(source, key, opts)
        |> reduce_association_filters(filter, source, key, term, opts)

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

      reducible_filter_entries?(term) ->
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

  defp reducible_filter_entries?(term) do
    (is_map(term) and not is_struct(term)) or Keyword.keyword?(term)
  end

  # defp filter_group_list?([]), do: true
  # defp filter_group_list?([head | _]) when (is_map(head) and not is_struct(head)) or Keyword.keyword?(head), do: true
  # defp filter_group_list?(_), do: false

  defp filter_group_list?(term) do
    is_list(term) and not Keyword.keyword?(term) and
      Enum.all?(term, fn e -> (is_map(e) and not is_struct(e)) or Keyword.keyword?(e) end)
  end

  defp association_filter?(source, key, term) do
    reducible_filter_entries?(term) and
      key in (CommonSchema.get_schema_reflection(source, :associations) || [])
  end

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
