defmodule EctoShorts.QueryBuilder.ExpressionBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Ecto Query expression builder API

  This module provides an api for composing ecto queries and building dynamic query expressions.
  """
  alias EctoShorts.CommonSchemas
  alias EctoShorts.QueryBuilder.QueryExpression

  @doc """
  ...
  """
  def apply_expressions(query, {key, values}, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, query, fn value, query ->
        apply_expressions(query, value, fun)
      end)
    else
      fun.(query, {key, values})
    end
  end

  def apply_expressions(query, {key, params}, fun) when is_map(params) do
    Enum.reduce(params, query, fn value, query ->
      apply_expressions(query, {key, value}, fun)
    end)
  end

  def apply_expressions(query, {key, value}, fun) do
    fun.(query, {key, value})
  end

  def apply_expressions(query, values, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, query, fn value, query ->
        apply_expressions(query, value, fun)
      end)
    else
      fun.(query, values)
    end
  end

  def apply_expressions(query, params, fun) when is_map(params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      apply_expressions(query, {key, value}, fun)
    end)
  end

  def apply_expressions(query, value, fun) do
    fun.(query, value)
  end

  defp has_params?([head | _]) when is_list(head) or is_map(head), do: true
  defp has_params?(_), do: false

  def join_association(query, current_binding, schema_module, key, params, fun) do
    source_key = field_source(schema_module, key)

    assoc_schema_module = get_ecto_association_schema(schema_module, source_key)

    {assoc_binding, params} = Map.pop(params, :as)

    assoc_binding = assoc_binding || named_binding(source_key)

    query
    |> QueryExpression.join(
      {current_binding, assoc_binding},
      {:association, source_key, Map.take(params, [:on, :qualifier, :prefix])}
    )
    |> fun.(assoc_binding, assoc_schema_module, Map.drop(params, [:on, :qualifier, :prefix]))
  end

  def join_subquery(
        query,
        current_binding,
        _schema_module,
        from,
        params,
        fun
      ) do
    subquery_schema_module = CommonSchemas.get_schema_queryable(from)

    {subquery_binding, params} = Map.pop(params, :as)

    subquery_binding =
      with nil <- subquery_binding do
        named_binding_from_module(subquery_schema_module)
      end

    query
    |> QueryExpression.join(
      {current_binding, subquery_binding},
      {:subquery, from, Map.take(params, [:on, :qualifier, :prefix])}
    )
    |> fun.(
      subquery_binding,
      subquery_schema_module,
      Map.drop(params, [:on, :qualifier, :prefix])
    )
  end

  @doc """
  ...
  """
  def select(query, current_binding, params) do
    apply_expressions(query, params, fn query, value ->
      QueryExpression.select(query, current_binding, value)
    end)
  end

  @doc """
  ...
  """
  def select_merge(query, current_binding, params) do
    apply_expressions(query, params, fn query, value ->
      QueryExpression.select_merge(query, current_binding, value)
    end)
  end

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

    dyn_expr = apply_expressions(nil, params, &apply_dynamic_where_expr(&1, current_binding, schema_module, &2))

    QueryExpression.or_where(query, nil, dyn_expr)
  end

  def or_where(query, current_binding, key, value) do
    or_where(query, current_binding, key, :==, value)
  end

  @doc """
  ...
  """
  def or_where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr = where_expr(nil, current_binding, schema_module, key, operator, value)

    QueryExpression.or_where(query, nil, dyn_expr)
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

    dyn_expr = apply_expressions(nil, params, &apply_dynamic_where_expr(&1, current_binding, schema_module, &2))

    QueryExpression.where(query, nil, dyn_expr)
  end

  @doc """
  ...
  """
  def where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr = where_expr(nil, current_binding, schema_module, key, operator, value)

    QueryExpression.where(query, nil, dyn_expr)
  end

  defp apply_dynamic_where_expr(dyn, current_binding, schema_module, {key, {operator, value}}) do
    where_expr(dyn, current_binding, schema_module, key, operator, value)
  end

  defp apply_dynamic_where_expr(dyn, current_binding, schema_module, {key, value}) do
    where_expr(dyn, current_binding, schema_module, key, :==, value)
  end

  defp where_expr(dyn, current_binding, schema_module, key, operator, value) do
    if field_type_of_array?(schema_module, key) do
      where_array_expr(dyn, current_binding, key, operator, value)
    else
      where_field_expr(dyn, current_binding, key, operator, value)
    end
  end

  defp where_array_expr(dyn, current_binding, key, operator, value) do
    if is_list(value) do
      QueryExpression.merge_dynamic(dyn, QueryExpression.Dynamic.Postgres.Array.where(current_binding, key, operator, value))
    else
      QueryExpression.merge_dynamic(dyn, QueryExpression.Dynamic.Postgres.Array.where(current_binding, value, operator, key))
    end
  end

  defp where_field_expr(dyn, current_binding, value, operator, key) do
    QueryExpression.merge_dynamic(dyn, QueryExpression.Dynamic.Postgres.Field.where(current_binding, value, operator, key))
  end

  defp field_source(_schema_module, key) do
    key
    # schema_module.__schema__(:field_source, key) || key
  end

  defp named_binding_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> named_binding()
  end

  defp named_binding(key) do
    :"ecto_shorts_#{key}"
  end

  defp get_ecto_association_schema(schema_module, key) do
    case schema_module.__schema__(:association, key) do
      %{through: [field1, field2]} ->
        schema_module
        |> get_ecto_association_schema(field1)
        |> get_ecto_association_schema(field2)

      %{related: related} ->
        related
    end
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
