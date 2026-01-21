defmodule EctoShorts.QueryBuilder.Filters do
  @moduledoc false

  alias Ecto.Query

  alias EctoShorts.CommonSchema
  alias EctoShorts.QueryBuilder.Dynamics

  require Ecto.Query

  @logger_prefix "EctoShorts.QueryBuilder.Filters"

  @default_operator :==
  @common_operators [:ids, :before, :after, :start_date, :end_date]
  @boolean_operators [:and, :or]

  def build_query(
        schema,
        filter,
        query,
        binding_selector,
        {bool_op, values},
        _opts
      )
      when bool_op in @boolean_operators and is_list(values) do
    dynamic = boolean_dynamic(schema, binding_selector, nil, {bool_op, values})

    compose(filter, query, dynamic)
  end

  def build_query(source, filter, query, binding_selector, {key, value}, _opts) do
    cond do
      key in @common_operators ->
        dynamic =
          source
          |> CommonSchema.get_schema_source()
          |> Dynamics.dynamic_field_expr(binding_selector, key, value)

        compose(filter, query, dynamic)

      is_nil(source) ->
        {operator, value} = normalize_operator_value(value)
        apply_filter(source, filter, query, binding_selector, key, {operator, value})

      key in CommonSchema.get_schema_reflection(source, :query_fields) ->
        {operator, value} = normalize_operator_value(value)
        apply_filter(source, filter, query, binding_selector, key, {operator, value})

      true ->
        warn_non_schema_key(source, key)
        query
    end
  end

  def build_query(_schema, filter, _query, _binding_selector, term, _opts) do
    raise ArgumentError,
          "Expected filter params for #{inspect(filter)} to be a map or keyword list, got: #{inspect(term)}"
  end

  defp apply_filter(
         schema,
         filter,
         query,
         binding_selector,
         field_key,
         {bool_op, values}
       )
       when bool_op in @boolean_operators do
    if composite?(values) do
      dynamic = boolean_dynamic(schema, binding_selector, nil, {bool_op, values})

      compose(filter, query, dynamic)
    else
      dynamic = boolean_dynamic(schema, binding_selector, field_key, {bool_op, values})

      compose(filter, query, dynamic)
    end
  end

  defp apply_filter(schema, filter, query, binding_selector, field_key, {operator, values})
       when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.reduce(values, query, fn value, updated_query ->
        dynamic = build_dynamic(schema, binding_selector, field_key, {operator, value})
        compose(filter, updated_query, dynamic)
      end)
    else
      dynamic = build_dynamic(schema, binding_selector, field_key, {operator, values})
      compose(filter, query, dynamic)
    end
  end

  defp apply_filter(schema, filter, query, binding_selector, field_key, {operator, params})
       when is_map(params) do
    Enum.reduce(params, query, fn value, updated_query ->
      dynamic = build_dynamic(schema, binding_selector, field_key, {operator, value})
      compose(filter, updated_query, dynamic)
    end)
  end

  defp apply_filter(schema, filter, query, binding_selector, field_key, {operator, field_value}) do
    dynamic = build_dynamic(schema, binding_selector, field_key, {operator, field_value})
    compose(filter, query, dynamic)
  end

  defp build_composite_dynamic(schema, binding_selector, enum) do
    Enum.reduce(enum, nil, fn {field_key, field_value}, dyn_left ->
      if is_nil(schema) or field_key in schema.__schema__(:query_fields) do
        build_schema_composite_dynamic(
          schema,
          binding_selector,
          {field_key, field_value},
          dyn_left
        )
      else
        warn_non_schema_key(schema, field_key)

        dyn_left
      end
    end)
  end

  defp build_schema_composite_dynamic(
         schema,
         binding_selector,
         {field_key, field_value},
         dyn_left
       ) do
    dyn_right =
      if map_or_kw?(field_value) do
        build_composite_boolean_dynamic(schema, binding_selector, field_key, field_value)
      else
        {operator, value} = normalize_operator_value(field_value)
        build_dynamic(schema, binding_selector, field_key, {operator, value})
      end

    merge_dynamic(dyn_left, :and, dyn_right)
  end

  defp build_composite_boolean_dynamic(schema, binding_selector, field_key, field_value) do
    Enum.reduce(field_value, nil, fn {operator, value}, dyn_left ->
      dyn_right =
        if operator in @boolean_operators and is_list(value) do
          boolean_dynamic(schema, binding_selector, field_key, {operator, value})
        else
          build_dynamic(schema, binding_selector, field_key, {operator, value})
        end

      merge_dynamic(dyn_left, :and, dyn_right)
    end)
  end

  defp boolean_dynamic(schema, binding_selector, field_key, {bool_op, values})
       when bool_op in @boolean_operators and is_list(values) do
    reduce_boolean(values, bool_op, fn value ->
      if composite?(values) do
        build_composite_dynamic(schema, binding_selector, value)
      else
        build_dynamic(schema, binding_selector, field_key, value)
      end
    end)
  end

  defp reduce_boolean(values, bool_op, fun) when is_list(values) and is_function(fun, 1) do
    Enum.reduce(values, nil, fn value, dyn_left ->
      dyn_right = fun.(value)
      merge_dynamic(dyn_left, bool_op, dyn_right)
    end)
  end

  defp build_dynamic(schema, binding_selector, field_key, field_value) do
    # Delegate dynamic expression compilation to the shared compiler so join :on
    # and where/or_where filters share the same operator surface.
    Dynamics.convert_params_to_dynamic(
      schema,
      nil,
      binding_selector,
      %{field_key => field_value}
    )
  end

  defp merge_dynamic(nil, _, right_dynamic) do
    right_dynamic
  end

  defp merge_dynamic(left_dynamic, :and, right_dynamic) do
    Query.dynamic([], ^left_dynamic and ^right_dynamic)
  end

  defp merge_dynamic(left_dynamic, :or, right_dynamic) do
    Query.dynamic([], ^left_dynamic or ^right_dynamic)
  end

  defp compose(_, query, nil) do
    query
  end

  defp compose(:or_where, query, dynamic) do
    Query.or_where(query, ^dynamic)
  end

  defp compose(:where, query, dynamic) do
    Query.where(query, ^dynamic)
  end

  defp composite?(values) when is_list(values) do
    case values do
      [v] -> is_map(v) or Keyword.keyword?(v)
      [v | _] -> is_map(v) or Keyword.keyword?(v)
      _ -> false
    end
  end

  defp map_or_kw?(term) do
    (is_map(term) and not is_struct(term)) or Keyword.keyword?(term)
  end

  defp normalize_operator_value(term) do
    case term do
      {nil, value} -> {@default_operator, value}
      {op, value} -> {op, value}
      value -> {@default_operator, value}
    end
  end

  defp warn_non_schema_key(schema, key) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected a query field for schema #{inspect(schema)}, got: #{inspect(key)}"
    )
  end
end
