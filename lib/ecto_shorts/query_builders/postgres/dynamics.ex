defmodule EctoShorts.QueryBuilder.Dynamics do
  @moduledoc false
  alias Ecto.Query

  alias EctoShorts.QueryBuilder.Dynamics.{
    ArrayExpr,
    CommonExpr,
    ScalarExpr
  }

  require Ecto.Query

  @default_operator :==
  @common_operators [:ids, :before, :after, :start_date, :end_date]
  @boolean_operators [:and, :or]

  # Shared "params -> dynamic" compiler.
  #
  # This function is intentionally generic so it can be used by joins (for :on)
  # and by any other query builder component that needs a dynamic expression.
  #
  # NOTE: This stays within the "dynamic-only" boundary: it does not add joins,
  # selects, ordering, etc.
  def convert_params_to_dynamic(schema, dynamic, binding_selector, params) do
    reduce_dynamic(schema, dynamic, binding_selector, params)
  end

  defp reduce_dynamic(schema, dynamic, binding_selector, {bool_op, values})
       when bool_op in @boolean_operators and is_list(values) do
    right_dynamic =
      reduce_boolean(values, bool_op, fn value ->
        reduce_dynamic(schema, nil, binding_selector, value)
      end)

    merge_dynamic(dynamic, :and, right_dynamic)
  end

  defp reduce_dynamic(schema, dynamic, binding_selector, {key, value}) do
    right_dynamic = dynamic_field_expr(schema, binding_selector, key, value)
    merge_dynamic(dynamic, :and, right_dynamic)
  end

  defp reduce_dynamic(schema, dynamic, binding_selector, enum) when is_map(enum) do
    Enum.reduce(enum, dynamic, fn pair, left_dynamic ->
      reduce_dynamic(schema, left_dynamic, binding_selector, pair)
    end)
  end

  defp reduce_dynamic(schema, dynamic, binding_selector, enum) when is_list(enum) do
    Enum.reduce(enum, dynamic, fn value, left_dynamic ->
      reduce_dynamic(schema, left_dynamic, binding_selector, value)
    end)
  end

  defp reduce_dynamic(_schema, _dynamic, _binding_selector, term) do
    raise ArgumentError,
          "Expected dynamic params to be a map, keyword list, or boolean expression, got: #{inspect(term)}"
  end

  @doc """
  Convert a field key and value to a dynamic expression.
  """
  def dynamic_field_expr(schema, binding_selector, key, value) do
    cond do
      key in @common_operators ->
        CommonExpr.dynamic_field_expr(binding_selector, key, value)

      map_or_kw?(value) ->
        # Support nested boolean ops at the field level, e.g.
        # %{published: %{or: [==: true, ==: false]}}
        Enum.reduce(value, nil, fn {operator, value}, left_dynamic ->
          right_dynamic =
            if operator in @boolean_operators and is_list(value) do
              boolean_dynamic(schema, binding_selector, key, {operator, value})
            else
              build_dynamic(schema, binding_selector, key, {operator, value})
            end

          merge_dynamic(left_dynamic, :and, right_dynamic)
        end)

      true ->
        {operator, value} = normalize_operator_value(value)
        build_dynamic(schema, binding_selector, key, {operator, value})
    end
  end

  defp boolean_dynamic(schema, binding_selector, key, {bool_op, values})
       when bool_op in @boolean_operators and is_list(values) do
    reduce_boolean(values, bool_op, fn value ->
      if composite?(values) do
        # values are maps/kw: each element is a composite branch
        reduce_dynamic(schema, nil, binding_selector, value)
      else
        # values are operator tuples: e.g. [==: true, ==: false]
        build_dynamic(schema, binding_selector, key, value)
      end
    end)
  end

  defp build_dynamic(schema, binding_selector, key, {operator, value}) do
    case schema_field_type(schema, key) do
      {:array, _} -> ArrayExpr.dynamic_field_expr(binding_selector, key, {operator, value})
      _ -> ScalarExpr.dynamic_field_expr(binding_selector, key, {operator, value})
    end
  end

  defp schema_field_type(nil, _key), do: nil

  defp schema_field_type(schema, key) when is_atom(schema) do
    schema.__schema__(:type, key)
  end

  defp schema_field_type(_, _) do
    nil
  end

  defp reduce_boolean(values, bool_op, fun) when is_list(values) and is_function(fun, 1) do
    Enum.reduce(values, nil, fn value, left_dynamic ->
      right_dynamic = fun.(value)
      merge_dynamic(left_dynamic, bool_op, right_dynamic)
    end)
  end

  defp merge_dynamic(nil, _, right_dynamic), do: right_dynamic

  defp merge_dynamic(left_dynamic, :and, right_dynamic),
    do: Query.dynamic([], ^left_dynamic and ^right_dynamic)

  defp merge_dynamic(left_dynamic, :or, right_dynamic),
    do: Query.dynamic([], ^left_dynamic or ^right_dynamic)

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
end
