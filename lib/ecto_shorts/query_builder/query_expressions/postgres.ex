defmodule EctoShorts.QueryBuilder.QueryExpressions.Postgres do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder.Helpers,
    QueryBuilder.QueryAPI,
    QueryBuilder.QueryExpressions.Postgres.Array,
    QueryBuilder.QueryExpressions.Postgres.Field
  }

  @doc """
  ...
  """
  def or_where(query, current_binding, params) when is_list(params) do
    if Keyword.keyword?(params) do
      or_where(query, current_binding, Map.new(params))
    else
      Enum.reduce(params, query, fn p, query ->
        or_where(query, current_binding, p)
      end)
    end
  end

  def or_where(query, current_binding, params) when is_map(params) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr =
      Helpers.apply_expressions(
        nil,
        params,
        &build_where_expr(&1, current_binding, schema_module, &2)
      )

    QueryAPI.or_where(query, nil, dyn_expr)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, key, value) do
    or_where(query, current_binding, key, :==, value)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr = build_where_expr(nil, current_binding, schema_module, key, operator, value)

    QueryAPI.or_where(query, nil, dyn_expr)
  end

  @doc """
  ...
  """
  def where(query, current_binding, params) when is_list(params) do
    if Keyword.keyword?(params) do
      where(query, current_binding, Map.new(params))
    else
      Enum.reduce(params, query, fn p, query ->
        where(query, current_binding, p)
      end)
    end
  end

  def where(query, current_binding, params) when is_map(params) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr =
      Helpers.apply_expressions(
        nil,
        params,
        &build_where_expr(&1, current_binding, schema_module, &2)
      )

    QueryAPI.where(query, nil, dyn_expr)
  end

  @doc """
  ...
  """
  def where(query, current_binding, key, value) do
    where(query, current_binding, key, :==, value)
  end

  @doc """
  ...
  """
  def where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr = build_where_expr(nil, current_binding, schema_module, key, operator, value)

    QueryAPI.where(query, nil, dyn_expr)
  end

  defp build_where_expr(dyn, current_binding, schema_module, {key, {operator, value}}) do
    build_where_expr(dyn, current_binding, schema_module, key, operator, value)
  end

  defp build_where_expr(dyn, current_binding, schema_module, {key, value}) do
    build_where_expr(dyn, current_binding, schema_module, key, :==, value)
  end

  defp build_where_expr(dyn, current_binding, schema_module, key, operator, value) do
    if field_type_of_array?(schema_module, key) do
      put_where_array_expr(dyn, current_binding, key, operator, value)
    else
      put_where_field_expr(dyn, current_binding, key, operator, value)
    end
  end

  defp put_where_array_expr(dyn, current_binding, key, operator, value) do
    if is_list(value) do
      QueryAPI.merge_dynamic(dyn, Array.where(current_binding, key, operator, value))
    else
      QueryAPI.merge_dynamic(dyn, Array.where(current_binding, value, operator, key))
    end
  end

  defp put_where_field_expr(dyn, current_binding, value, operator, key) do
    QueryAPI.merge_dynamic(dyn, Field.where(current_binding, value, operator, key))
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
