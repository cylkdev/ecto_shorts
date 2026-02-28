defmodule EctoShorts.CommonFilters.Filter do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonFilters
  alias EctoShorts.QueryProvider
  alias EctoShorts.Dynamics

  alias Ecto.Query
  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Filter"

  @boolean_operators [:and, :or]

  @query_filters [
    :except,
    :except_all,
    :exclude,
    :first,
    :intersect,
    :intersect_all,
    :union,
    :union_all,
    :last,
    :lock,
    :limit,
    :offset,
    :put_query_prefix,
    :recursive_ctes,
    :reverse_order
  ]

  @custom_filters [:ids, :before, :after, :start_date, :end_date, :exists]

  @doc "Applies the given filter to the query."
  def build(schema_source, filter, query, binding_selector, {boolean_operator, params}, opts)
      when boolean_operator in @boolean_operators and is_list(params) do
    dyn =
      Dynamics.convert_to_dynamic(
        schema_source,
        binding_selector,
        {boolean_operator, params},
        opts
      )

    apply_where_expr(filter, query, dyn)
  end

  def build(schema_source, filter, query, binding_selector, {key, params}, opts)
      when key in @custom_filters do
    dyn =
      schema_source
      |> CommonSchema.get_schema_source()
      |> Dynamics.convert_to_dynamic(binding_selector, {key, params}, opts)

    apply_where_expr(filter, query, dyn)
  end

  def build(
        _schema_source,
        filter,
        query,
        _binding_selector,
        {:dynamic, value},
        _opts
      )
      when filter in [:where, :or_where] do
    if is_struct(value, Ecto.Query.DynamicExpr) do
      apply_where_expr(filter, query, value)
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :dynamic payload to be an Ecto.Query.DynamicExpr, got: #{inspect(value)}"
      )

      query
    end
  end

  def build(schema_source, filter, query, binding_selector, value, opts)
      when filter in @query_filters do
    apply_expr(schema_source, filter, query, binding_selector, value, opts)
  end

  def build(schema_source, filter, query, binding_selector, {key, value}, opts) do
    cond do
      schemaless_source?(schema_source) ->
        build_field(schema_source, filter, query, binding_selector, key, value, opts)

      key in CommonSchema.get_schema_reflection(schema_source, :query_fields) ->
        build_field(schema_source, filter, query, binding_selector, key, value, opts)

      true ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected a query field for schema #{inspect(schema_source)}, got: #{inspect(key)}"
        )

        query
    end
  end

  def build(_schema, _filter, query, _binding_selector, term, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected params to be a map or keyword list, got: #{inspect(term)}"
    )

    query
  end

  defp schemaless_source?(source), do: not source_has_schema?(source)

  defp source_has_schema?({_, schema}), do: is_atom(schema) and schema !== nil
  defp source_has_schema?(_), do: false

  defp build_field(schema_source, filter, query, binding_selector, key, value, opts) do
    dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, {key, value}, opts)
    apply_where_expr(filter, query, dyn)
  end

  defp apply_expr(_schema_source, :exclude, query, _binding_selector, entries, _opts) do
    entries
    |> List.wrap()
    |> Enum.reduce(query, fn filter, q2 -> Query.exclude(q2, filter) end)
  end

  defp apply_expr(schema_source, :except, query, _binding_selector, value, opts) do
    expr = to_query(schema_source, value, opts)
    Query.except(query, ^expr)
  end

  defp apply_expr(schema_source, :except_all, query, _binding_selector, value, opts) do
    expr = to_query(schema_source, value, opts)
    Query.except_all(query, ^expr)
  end

  defp apply_expr(schema_source, :intersect, query, _binding_selector, value, opts) do
    expr = to_query(schema_source, value, opts)
    Query.intersect(query, ^expr)
  end

  defp apply_expr(schema_source, :intersect_all, query, _binding_selector, value, opts) do
    expr = to_query(schema_source, value, opts)
    Query.intersect_all(query, ^expr)
  end

  defp apply_expr(schema_source, :union, query, _binding_selector, value, opts) do
    expr = to_query(schema_source, value, opts)
    Query.union(query, ^expr)
  end

  defp apply_expr(schema_source, :union_all, query, _binding_selector, value, opts) do
    expr = to_query(schema_source, value, opts)
    Query.union_all(query, ^expr)
  end

  defp apply_expr(schema_source, :first, query, binding_selector, limit, opts) do
    apply_expr(schema_source, :limit, query, binding_selector, limit, opts)
  end

  defp apply_expr(schema_source, :last, query, binding_selector, entries, opts)
       when is_map(entries) or is_list(entries) do
    Enum.reduce(entries, query, fn entry, q ->
      apply_expr(schema_source, :last, q, binding_selector, entry, opts)
    end)
  end

  defp apply_expr(
         schema_source,
         :last,
         query,
         _binding_selector,
         {sort_key, limit},
         _opts
       ) do
    # Applies `:last` pagination by selecting the last `limit` rows
    # according to `sort_keys`.
    #
    # First, any existing `order_by` is removed and the query is ordered
    # in descending order by `sort_keys` so the last rows come first;
    # then `limit` is applied and the result is wrapped in a subquery.
    #
    # Finally, the outer query is ordered in ascending order by the same
    # `sort_keys` so the returned rows are presented in the expected order.
    # This reduces over `sort_keys` to support composite primary keys
    # (multi-field ordering).

    sort_keys =
      if is_nil(sort_key) do
        List.wrap(CommonSchema.get_schema_reflection(schema_source, :primary_key) || :id)
      else
        List.wrap(sort_key)
      end

    subquery =
      sort_keys
      |> Enum.reduce(Query.exclude(query, :order_by), &Query.order_by(&2, desc: ^&1))
      |> Query.from(limit: ^limit)
      |> Query.subquery()

    Enum.reduce(sort_keys, subquery, &Query.order_by(&2, asc: ^&1))
  end

  defp apply_expr(schema_source, :last, query, binding_selector, limit, opts) do
    apply_expr(schema_source, :last, query, binding_selector, {nil, limit}, opts)
  end

  defp apply_expr(_schema_source, :lock, query, _binding_selector, value, _opts)
       when is_function(value, 1) do
    value.(query)
  end

  defp apply_expr(schema_source, :lock, query, binding_selector, params, opts)
       when is_map(params) and not is_struct(params) do
    apply_expr(schema_source, :lock, query, binding_selector, Map.to_list(params), opts)
  end

  defp apply_expr(_schema_source, :lock, query, binding_selector, params, opts)
       when is_list(params) do
    if Keyword.keyword?(params) do
      apply_lock_from_resolver(query, binding_selector, params, opts)
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :lock params to be a unary function or a keyword/map resolver payload, got: #{inspect(params)}"
      )

      query
    end
  end

  defp apply_expr(_schema_source, :lock, query, _binding_selector, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :lock params to be a unary function or a keyword/map resolver payload, got: #{inspect(value)}"
    )

    query
  end

  defp apply_expr(_schema_source, :limit, query, _binding_selector, value, _opts) do
    Query.limit(query, ^value)
  end

  defp apply_expr(_schema_source, :offset, query, _binding_selector, value, _opts) do
    Query.offset(query, ^value)
  end

  defp apply_expr(_schema_source, :put_query_prefix, query, _binding_selector, value, _opts)
       when is_binary(value) do
    Query.put_query_prefix(query, value)
  end

  defp apply_expr(_schema_source, :put_query_prefix, query, _binding_selector, value, _opts) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :put_query_prefix value to be a string, got: #{inspect(value)}"
    )

    query
  end

  defp apply_expr(
         _schema_source,
         :recursive_ctes,
         query,
         _binding_selector,
         value,
         _opts
       )
       when is_boolean(value) do
    Query.recursive_ctes(query, value)
  end

  defp apply_expr(
         _schema_source,
         :recursive_ctes,
         query,
         _binding_selector,
         value,
         _opts
       ) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :recursive_ctes value to be a boolean, got: #{inspect(value)}"
    )

    query
  end

  defp apply_expr(
         _schema_source,
         :reverse_order,
         query,
         _binding_selector,
         _value,
         _opts
       ) do
    Query.reverse_order(query)
  end

  defp apply_where_expr(_, query, nil) do
    query
  end

  defp apply_where_expr(:or_where, query, dyn) do
    Query.or_where(query, ^dyn)
  end

  defp apply_where_expr(:where, query, dyn) do
    Query.where(query, ^dyn)
  end

  defp apply_lock_from_resolver(query, binding_selector, params, opts) do
    params =
      if is_map(params) and not is_struct(params) do
        Map.to_list(params)
      else
        params
      end

    lock_name = Keyword.get(params, :name)
    lock_values = Keyword.get(params, :values, [])

    if is_nil(lock_name) do
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :lock resolver payload to have a :name key, got: #{inspect(params)}"
      )

      query
    else
      case QueryProvider.resolve_expression(binding_selector, lock_name, lock_values, opts) do
        {:ok, lock_builder} when is_function(lock_builder, 1) ->
          lock_builder.(query)

        {:ok, other} ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :lock resolver to return {:ok, (Ecto.Query.t() -> Ecto.Query.t())}, got: #{inspect(other)}"
          )

          query

        {:error, reason} ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Lock expression callback returned error for key #{inspect(lock_name)}: #{inspect(reason)}"
          )

          query

        other ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :lock resolver callback to return {:ok, query_builder_fun} | {:error, reason}, got: #{inspect(other)}"
          )

          query
      end
    end
  end

  defp to_query(schema_source, value, opts) do
    if is_struct(value, Ecto.Query) do
      value
    else
      CommonFilters.convert_params_to_filter(schema_source, value, opts)
    end
  end
end
