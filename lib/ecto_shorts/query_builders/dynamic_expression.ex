defmodule EctoShorts.QueryBuilders.DynamicExpression do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """
  alias EctoShorts.{
    CommonSchemas,
    QueryBuilders.DynamicExpression.Postgres,
    QueryBuilders.Expression
  }

  @doc """
  ...
  """
  def apply_filters(query, {key, values}, fun) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      apply_filters(query, {key, value}, fun)
    end)
  end

  def apply_filters(query, {key, value}, fun) do
    fun.(query, {key, value})
  end

  def apply_filters(query, values, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, query, fn value, query ->
        apply_filters(query, value, fun)
      end)
    else
      fun.(query, values)
    end
  end

  def apply_filters(query, params, fun) when is_map(params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      apply_filters(query, {key, value}, fun)
    end)
  end

  def apply_filters(query, value, fun) do
    fun.(query, value)
  end

  defp has_params?([head | _]) when is_list(head) or is_map(head), do: true
  defp has_params?(_), do: false

  @doc """
  ...
  """
  def or_where(query, current_binding, params) when is_map(params) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    Enum.reduce(params, query, fn value, query ->
      reduce_or_where(query, current_binding, schema_module, value)
    end)
  end

  defp reduce_or_where(query, current_binding, schema_module, {key, value}) do
    apply_filters(query, value, fn query, value ->
      if field_type_of_array?(schema_module, key) do
        or_where_array(query, current_binding, schema_module, key, value)
      else
        or_where_field(query, current_binding, schema_module, key, value)
      end
    end)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    if field_type_of_array?(schema_module, key) do
      or_where_array(query, current_binding, schema_module, key, {operator, value})
    else
      or_where_field(query, current_binding, schema_module, key, {operator, value})
    end
  end

  defp or_where_array(query, current_binding, _schema_module, key, {operator, value}) do
    if is_list(value) do
      Expression.or_where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, key, operator, value)
      )
    else
      Expression.or_where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, value, operator, key)
      )
    end
  end

  defp or_where_array(query, current_binding, _schema_module, key, value) do
    if is_list(value) do
      Expression.or_where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, key, :==, value)
      )
    else
      Expression.or_where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, value, :==, key)
      )
    end
  end

  defp or_where_field(query, current_binding, _schema_module, key, {operator, value}) do
    Expression.or_where(
      query,
      nil,
      Postgres.Field.dynamic_expression(current_binding, key, operator, value)
    )
  end

  defp or_where_field(query, current_binding, _schema_module, key, value) do
    Expression.or_where(
      query,
      nil,
      Postgres.Field.dynamic_expression(current_binding, key, :==, value)
    )
  end

  @doc """
  ...
  """
  def where(query, current_binding, params) when is_map(params) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    Enum.reduce(params, query, fn value, query ->
      reduce_where(query, current_binding, schema_module, value)
    end)
  end

  defp reduce_where(query, current_binding, schema_module, {key, value}) do
    apply_filters(query, value, fn query, value ->
      if field_type_of_array?(schema_module, key) do
        where_array(query, current_binding, schema_module, key, value)
      else
        where_field(query, current_binding, schema_module, key, value)
      end
    end)
  end

  @doc """
  ...
  """
  def where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    if field_type_of_array?(schema_module, key) do
      where_array(query, current_binding, schema_module, key, {operator, value})
    else
      where_field(query, current_binding, schema_module, key, {operator, value})
    end
  end

  defp where_array(query, current_binding, _schema_module, key, {operator, value}) do
    if is_list(value) do
      Expression.where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, key, operator, value)
      )
    else
      Expression.where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, value, operator, key)
      )
    end
  end

  defp where_array(query, current_binding, _schema_module, key, value) do
    if is_list(value) do
      Expression.where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, key, :==, value)
      )
    else
      Expression.where(
        query,
        nil,
        Postgres.Array.dynamic_expression(current_binding, value, :==, key)
      )
    end
  end

  defp where_field(query, current_binding, _schema_module, key, {operator, value}) do
    Expression.where(
      query,
      nil,
      Postgres.Field.dynamic_expression(current_binding, key, operator, value)
    )
  end

  defp where_field(query, current_binding, _schema_module, key, value) do
    Expression.where(
      query,
      nil,
      Postgres.Field.dynamic_expression(current_binding, key, :==, value)
    )
  end

  defp field_type(schema_module, key) do
    schema_module.__schema__(:type, key)
  end

  defp field_type_of_array?(schema_module, key) do
    case field_type(schema_module, key) do
      {:array, _} -> true
      _ -> false
    end
  end
end
