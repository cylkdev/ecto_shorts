defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Converts maps and keyword lists of filter params into `Ecto.Query` structs.

  Let's explore how you can build queries from data structures instead of
  writing Ecto query macros by hand. We'll pass a schema module (or
  `{source, schema}` tuple) together with a map or keyword list of params
  to `convert_params_to_filter/3` and receive an `Ecto.Query` back.

  ## Getting started

  Let's see how this works with a simple example:

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{title: "Hello", published: true, limit: 10}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        where: p0.title == ^"Hello" and p0.published == ^true,
        limit: ^10>

  You can see how the map parameters are automatically converted into
  appropriate Ecto query clauses. The `title` and `published` fields become
  `WHERE` conditions, while `limit` becomes a query operation.

  ## Filter categories

  We split params into two categories to make query building intuitive:

  * **Schema filters** - keys that match schema field names or associations
    become `WHERE` clauses. You can nest these under `:or_where` for
    `OR WHERE` clauses when you need alternative conditions.

  * **Query filters** - Reserved keys that map to Ecto query operations:

    - `:distinct`
    - `:group_by`
    - `:having`
    - `:or_having`
    - `:join`
    - `:order_by`
    - `:prepend_order_by`
    - `:preload`
    - `:select`
    - `:select_merge`
    - `:limit`
    - `:offset`
    - `:lock`
    - `:union`
    - `:union_all`
    - `:except`
    - `:except_all`
    - `:intersect`
    - `:intersect_all`
    - `:exclude`
    - `:first`
    - `:last`
    - `:put_query_prefix`
    - `:recursive_ctes`
    - `:reverse_order`
    - `:subquery`
    - `:with_cte`
    - `:with_named_binding`
    - `:with_ties`
    - `:windows`
    - `:update`

  ## Binding targeting

  Sometimes you need to target a specific query binding. You can do this
  using the `:bind` key:

      %{bind: %{as: %{post: %{title: "Hello"}}}}  # named binding
      %{bind: %{at: %{2 => %{title: "Hello"}}}}   # positional binding

  This is particularly useful when you have complex queries with multiple
  joins and need to specify which binding you're referring to. This works
  for existing queries as well as new queries which allows you to easily
  build data-driven workflows on top of EctoShorts.

  Unknown keys that are not schema fields are logged as warnings and
  skipped for that filter entry.

  Invalid query-building payloads are also logged as warnings and skipped for
  the failing operation, so the rest of the query can continue building.

  ## Boolean operators

  Let's see how you can combine conditions using `:and` and `:or` operators:

      %{or: [%{published: true}, %{published: false}]}
      %{and: [%{title: "Hi"}, %{published: true}]}

  These operators give you fine-grained control over your query logic,
  allowing you to build complex conditions while keeping your code readable.

  See also `EctoShorts.Dynamics`, `EctoShorts.CommonFilters.Having`, and
  `EctoShorts.Actions` for more advanced query building capabilities.
  """

  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonQuery

  alias EctoShorts.CommonFilters.{
    BindingParams,
    Distinct,
    Filter,
    GroupBy,
    Having,
    Join,
    OrderBy,
    Preload,
    Select,
    SubQuery,
    WithCte,
    WithNamedBinding,
    WithTies,
    Windows,
    Update
  }

  alias EctoShorts.Logger
  alias EctoShorts.SchemaHelpers

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_selector_key :bind
  @default_binding_selector {:as, nil}

  @where :where
  @map_payload_helper_operators [:datetime_add, :date_add, :from_now, :ago]

  @schema_filters [:where, :or_where]

  @query_filters [
    :distinct,
    :except,
    :except_all,
    :exclude,
    :first,
    :group_by,
    :having,
    :or_having,
    :intersect,
    :intersect_all,
    :union,
    :union_all,
    :join,
    :last,
    :lock,
    :limit,
    :offset,
    :put_query_prefix,
    :order_by,
    :windows,
    :reverse_order,
    :prepend_order_by,
    :preload,
    :recursive_ctes,
    :select,
    :select_merge,
    :subquery,
    :with_cte,
    :with_named_binding,
    :with_ties,
    :update
  ]

  @doc """
  Converts filter params into an `Ecto.Query`.

  Let's see how we can transform your filter parameters into a fully-formed
  Ecto query. You'll provide a `source` (schema module, `{source, schema}` tuple,
  or `Ecto.Query`), `params` (map or keyword list of filter params), and optional
  `opts` that get forwarded to all sub-query builders.

  We automatically convert schema field keys into `WHERE` conditions for you.
  Query filter keys like `:limit`, `:order_by`, `:preload`, etc. are applied as
  the corresponding Ecto query operations. If we encounter unknown keys that aren't
  in the schema's `:query_fields`, we'll log warnings and skip only the failing
  filter operation. Invalid query-building operations also log warnings and are
  skipped instead of raising.

  You'll receive back an `Ecto.Query` struct with all your parameters applied,
  ready to use with your repository.

  ## Options

  * `:repo` - the `Ecto.Repo` module used to resolve the dynamic expression
    adapter. Defaults to `EctoShorts.Config.repo/0`.
  * `:dynamic_adapter` - a module implementing `EctoShorts.Dynamics.Adapter`
    for this call. Defaults to resolved from repo.
  * `:query_fields` - list of field atoms to limit which fields are accepted
    as schema filters.

  ## Examples

  Let's start with a simple field filter:

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{title: "Hello"}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.title == ^"Hello">

  Now let's see how we can combine conditions with OR logic:

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{published: true, or_where: %{published: false}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        where: p0.published == ^true or p0.published == ^false>

  Finally, let's add some query operations like ordering and limiting:

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{limit: 5, order_by: {:desc, :inserted_at}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        order_by: [desc: p0.inserted_at], limit: ^5>

  See also `EctoShorts.Actions.all/3`, `EctoShorts.Dynamics`, and
  `EctoShorts.CommonFilters.Having` for more ways to work with queries.
  """
  @spec convert_params_to_filter(
          source :: module() | {binary(), module()} | Ecto.Query.t(),
          params :: map() | keyword(),
          opts :: keyword()
        ) :: Ecto.Query.t()
  def convert_params_to_filter(source, params, opts \\ [])

  def convert_params_to_filter(source, params, opts) when is_map(params) do
    convert_params_to_filter(source, Map.to_list(params), opts)
  end

  def convert_params_to_filter(source, entries, opts) when is_list(entries) do
    query = CommonSchema.to_query(source)

    has_source_or_query? =
      Keyword.has_key?(entries, :source) or Keyword.has_key?(entries, :query)

    if Keyword.keyword?(entries) do
      {schema_source, params} = Keyword.pop(entries, :source, source)

      {other_params, params} = Keyword.pop(params, :query, [])

      normalized_source = CommonSchema.normalize_source(schema_source)

      merged_params =
        other_params
        |> ensure_kw()
        |> Keyword.merge(params)

      merged_params =
        case {has_source_or_query?, normalized_source} do
          {true, {_table, nil}} ->
            if Keyword.has_key?(merged_params, :select) do
              merged_params
            else
              Keyword.put(merged_params, :select, true)
            end

          _ ->
            merged_params
        end

      do_convert(normalized_source, query, merged_params, opts)
    else
      Enum.reduce(entries, query, fn entry, query_acc ->
        do_convert(source, query_acc, entry, opts)
      end)
    end
  end

  defp ensure_kw(map) when is_map(map), do: Map.to_list(map)

  defp ensure_kw(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
    else
      Logger.warning(@logger_prefix, "Expected params to be a keyword list, got: #{inspect(list)}")
      []
    end
  end

  defp ensure_kw(term) do
    Logger.warning(@logger_prefix, "Expected params to be a keyword list, got: #{inspect(term)}")
    []
  end

  defp do_convert(schema_source, query, params, opts) do
    normalized_params = normalize_filter_params(params)

    if is_map(normalized_params) or Keyword.keyword?(normalized_params) do
      create_schema_filter(
        schema_source,
        query,
        @default_binding_selector,
        @where,
        normalized_params,
        opts
      )
    else
      Logger.warning(@logger_prefix, "Expected params to be a map or list, got: #{inspect(params)}")
      query
    end
  end

  @doc false
  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        _filter_op,
        {key, value},
        opts
      )
      when key in @schema_filters do
    reduce_schema_filter_params(schema_source, query, binding_selector, key, value, opts)
  end

  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {@binding_selector_key, bind_params},
        opts
      ) do
    safe_query_operation(query, filter_op, {@binding_selector_key, bind_params}, fn ->
      BindingParams.build_binding_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        bind_params,
        opts
      )
    end)
  end

  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {key, value},
        opts
      ) do
    safe_query_operation(query, filter_op, {key, value}, fn ->
      query_filters = Keyword.get(opts, :query_filters, @query_filters)

      if key in query_filters do
        build_query(schema_source, query, binding_selector, key, value, opts)
      else
        reduce_default_filter_params(
          schema_source,
          query,
          binding_selector,
          filter_op,
          key,
          value,
          opts
        )
      end
    end)
  end

  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts)
      when is_map(params) do
    create_schema_filter(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(params),
      opts
    )
  end

  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts)
      when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {key, value}, query_acc ->
        create_schema_filter(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )
      end)
    else
      safe_query_operation(query, filter_op, params, fn ->
        build_query(schema_source, query, binding_selector, filter_op, params, opts)
      end)
    end
  end

  @doc false
  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts) do
    build_query(schema_source, query, binding_selector, filter_op, params, opts)
  end

  defp reduce_schema_filter_params(schema_source, query, binding_selector, filter_op, value, opts) do
    if is_map(value) or is_list(value) do
      Enum.reduce(value, query, fn entry, query_acc ->
        safe_query_operation(query_acc, filter_op, entry, fn ->
          build_schema_filters(schema_source, query_acc, binding_selector, filter_op, entry, opts)
        end)
      end)
    else
      Logger.warning(
        @logger_prefix,
        "Expected params for #{filter_op} to be a map or keyword list, got: #{inspect(value)}"
      )

      query
    end
  end

  defp reduce_default_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         key,
         value,
         opts
       ) do
    case CommonSchema.get_schema_reflection(schema_source, :associations) do
      nil ->
        build_schema_filters(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )

      assocs ->
        if key in assocs do
          build_join_filters(
            schema_source,
            query,
            binding_selector,
            filter_op,
            key,
            value,
            opts
          )
        else
          build_schema_filters(
            schema_source,
            query,
            binding_selector,
            filter_op,
            {key, value},
            opts
          )
        end
    end
  end

  defp build_join_filters(
         schema_source,
         query,
         binding_selector,
         filter,
         assoc_key,
         params,
         opts
       ) do
    if Keyword.keyword?(params) do
      assoc_schema =
        case schema_source do
          {_, parent_schema} -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
          parent_schema -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
        end

      joined_query =
        build_query(
          schema_source,
          query,
          binding_selector,
          :join,
          [{assoc_key, params}],
          opts
        )

      {join_binding_mode, join_binding_target} =
        if Keyword.has_key?(params, :as) do
          {:as, Keyword.get(params, :as, nil)}
        else
          {:at, CommonQuery.query_binding_count(joined_query)}
        end

      create_schema_filter(
        assoc_schema,
        joined_query,
        {join_binding_mode, join_binding_target},
        filter,
        Keyword.drop(params, [:as, :on, :type]),
        opts
      )
    else
      Logger.warning(
        @logger_prefix,
        "Expected association params for #{inspect(assoc_key)} to be a keyword list, got: #{inspect(params)}"
      )

      query
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) and
              (is_map_key(value, :datetime_add) or is_map_key(value, :date_add) or
                 is_map_key(value, :from_now) or is_map_key(value, :ago)) do
    build_query(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) do
    create_schema_filter(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, Map.to_list(value)},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, query, fn {key2, value2}, query_acc ->
        create_schema_filter(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, {key2, value2}},
          opts
        )
      end)
    else
      build_query(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {key, value},
        opts
      )
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    build_query(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  @query_builder_modules %{
    distinct: Distinct,
    group_by: GroupBy,
    having: Having,
    or_having: Having,
    join: Join,
    order_by: OrderBy,
    prepend_order_by: OrderBy,
    preload: Preload,
    select: Select,
    select_merge: Select,
    update: Update,
    windows: Windows,
    with_cte: WithCte,
    with_named_binding: WithNamedBinding,
    with_ties: WithTies
  }

  defp build_query(schema_source, query, binding_selector, :subquery, params, opts)
       when is_map(params) or is_list(params) do
    binding_source = to_binding_source(schema_source, query, binding_selector)

    filtered_query =
      create_schema_filter(
        schema_source,
        query,
        binding_selector,
        @where,
        params,
        opts
      )

    SubQuery.build(
      binding_source,
      :subquery,
      filtered_query,
      binding_selector,
      params,
      opts
    )
  end

  defp build_query(_schema_source, query, _binding_selector, :subquery, params, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :subquery params to be a map or keyword list, got: #{inspect(params)}"
    )

    query
  end

  defp build_query(schema_source, query, binding_selector, filter_op, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    module = Map.get(@query_builder_modules, filter_op, Filter)
    module.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp safe_query_operation(query, filter_op, params, callback) do
    callback.()
  rescue
    exception ->
      Logger.warning(
        @logger_prefix,
        "Skipping #{inspect(filter_op)} operation for #{inspect(params)} due to query-building error: #{Exception.message(exception)}"
      )

      query
  catch
    kind, reason ->
      Logger.warning(
        @logger_prefix,
        "Skipping #{inspect(filter_op)} operation for #{inspect(params)} due to #{kind}: #{inspect(reason)}"
      )

      query
  end

  defp to_binding_source(schema_source, _query, {:as, nil}) do
    schema_source
  end

  defp to_binding_source(_schema_source, query, {_binding_mode, binding_target}) do
    CommonQuery.get_query_binding_source(query, binding_target)
  end

  defp normalize_filter_params({k, v})
       when k in @map_payload_helper_operators and is_map(v) and not is_struct(v) do
    {k, v}
  end

  defp normalize_filter_params({k, v}) when is_map(v) or is_list(v) do
    {k, normalize_filter_params(v)}
  end

  defp normalize_filter_params(map) when is_map(map) and not is_struct(map) do
    map
    |> Map.to_list()
    |> normalize_filter_params()
  end

  defp normalize_filter_params(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
      |> sort_params()
      |> Enum.map(fn {k, v} -> {k, normalize_filter_params(v)} end)
    else
      list
    end
  end

  defp normalize_filter_params(term) do
    term
  end

  defp sort_params(params) do
    where_filters = Keyword.take(params, [:where])

    or_where_filters = Keyword.take(params, [:or_where])

    terminal_filters =
      Enum.flat_map([:last, :subquery], fn key ->
        case List.keyfind(params, key, 0) do
          nil -> []
          entry -> [entry]
        end
      end)

    # Regular field filters should be processed with where_filters
    # since they can contain implicit WHERE clauses and must come
    # before or_where filters
    rest = Keyword.drop(params, [:where, :or_where, :last, :subquery])

    where_filters
    |> Kernel.++(rest)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
