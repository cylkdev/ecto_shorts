defmodule EctoShorts.CommonFiltersRc.Schema do
  @moduledoc """
  ...
  """

  alias EctoShorts.CommonSchemas
  alias EctoShorts.CommonFiltersRc.QueryExpression

  def build_query(query, params) do
    query
    |> CommonSchemas.get_schema_query()
    |> build_query(CommonSchemas.get_schema_queryable(query), params)
  end

  def build_query(query, schema_module, params) do
    {current_binding, params} = Map.pop(params, :as)

    query
    |> QueryExpression.from(as: current_binding)
    |> build_query(schema_module, params, current_binding)
  end

  # with_named_binding

  def build_query(query, schema_module, {key, {:with_named_binding, params}}, _current_binding) do
    Enum.reduce(params, query, fn {binding_alias, value}, query ->
      build_query(query, schema_module, {key, value}, binding_alias)
    end)
  end

  def build_query(query, schema_module, {:with_named_binding, params}, _current_binding) do
    Enum.reduce(params, query, fn {binding_alias, value}, query ->
      build_query(query, schema_module, value, binding_alias)
    end)
  end

  # join

  def build_query(query, schema_module, {:join, {:association, {key, params}}}, current_binding) do
    ecto_assoc = schema_module.__schema__(:association, key)

    assoc_schema = ecto_assoc.queryable

    {join_binding, params} = Map.pop(params, :as)

    join_binding = join_binding || QueryExpression.named_binding_atom(key)

    query
    |> QueryExpression.join(:association, key, Map.take(params, [:on, :prefix, :qualifier]), join_binding, current_binding)
    |> build_query(assoc_schema, Map.drop(params, [:on, :prefix, :qualifier]), join_binding)
  end

  def build_query(query, schema_module, {:join, {:association, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:join, {:association, value}}, current_binding)
    end)
  end

  def build_query(query, schema_module, {:join, {:association, key}}, current_binding) do
    build_query(query, schema_module, {:join, {:association, {key, %{}}}}, current_binding)
  end

  def build_query(query, _schema_module, {:join, {:subquery, params}}, current_binding) when is_map(params) do
    {current_binding, params} = Map.pop(params, :with_named_binding, current_binding)

    {join_binding, params} = Map.pop(params, :as)

    from_query = params.query

    from_schema_module = EctoShorts.CommonSchemas.get_schema_queryable(from_query)

    join_binding =
      if join_binding do
        join_binding
      else
        from_schema_module
        |> Module.split()
        |> List.last()
        |> Macro.underscore()
        |> QueryExpression.named_binding_atom()
      end

    query
    |> QueryExpression.join(:subquery, from_query, Map.take(params, [:on, :prefix, :qualifier]), join_binding, current_binding)
    |> build_query(from_schema_module, Map.drop(params, [:on, :prefix, :qualifier]), join_binding)
  end

  def build_query(query, schema_module, {:join, {:subquery, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:join, {:subquery, value}}, current_binding)
    end)
  end

  def build_query(query, schema_module, {:join, {:subquery, query}}, current_binding) do
    build_query(query, schema_module, {:join, {:subquery, %{query: query}}}, current_binding)
  end

  def build_query(query, schema_module, {:join, values}, current_binding) do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:join, value}, current_binding)
    end)
  end

  # select

  def build_query(query, _schema_module, {:select, {type, keys}}, current_binding) do
    QueryExpression.select(query, type, keys, current_binding)
  end

  def build_query(query, _schema_module, {:select, true}, current_binding) do
    QueryExpression.select(query, current_binding)
  end

  def build_query(query, schema_module, {:select, values}, current_binding) when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.reduce(values, query, fn {key, value}, query ->
        build_query(query, schema_module, {:select, {key, value}}, current_binding)
      end)
    else
      QueryExpression.select(query, :map, values, current_binding)
    end
  end

  def build_query(query, schema_module, {:select, params}, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, schema_module, {:select, {key, value}}, current_binding)
    end)
  end

  # select_merge

  def build_query(query, _schema_module, {:select_merge, true}, current_binding) do
    QueryExpression.select_merge(query, current_binding)
  end

  def build_query(query, _schema_module, {:select_merge, keys}, current_binding) do
    QueryExpression.select_merge(query, keys, current_binding)
  end

  # or_where

  def build_query(query, _schema_module, {:or_where, {schema_field, {operation, {:parent_as, {parent_binding, parent_field}}}}}, current_binding) do
    QueryExpression.or_where(query, schema_field, operation, {:parent_as, {parent_binding, parent_field}}, current_binding)
  end

  def build_query(query, schema_module, {:or_where, {schema_field, {operation, {:parent_as, params}}}}, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, schema_module, {:or_where, {schema_field, {operation, {:parent_as, {key, value}}}}}, current_binding)
    end)
  end

  def build_query(query, schema_module, {:or_where, {schema_field, {operation, values}}}, current_binding) when is_list(values) or is_map(values)  do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:or_where, {schema_field, {operation, value}}}, current_binding)
    end)
  end

  def build_query(query, _schema_module, {:or_where, {schema_field, {operation, value}}}, current_binding) do
    QueryExpression.or_where(query, schema_field, operation, value, current_binding)
  end

  def build_query(query, schema_module, {:or_where, {schema_field, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:or_where, {schema_field, value}}, current_binding)
    end)
  end

  def build_query(query, _schema_module, {:or_where, {schema_field, value}}, current_binding) do
    QueryExpression.or_where(query, schema_field, :==, value, current_binding)
  end

  def build_query(query, schema_module, {:or_where, values}, current_binding)  do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:or_where, value}, current_binding)
    end)
  end

  # where

  def build_query(query, _schema_module, {:where, {schema_field, {operation, {:parent_as, {parent_binding, parent_field}}}}}, current_binding) do
    QueryExpression.where(query, schema_field, operation, {:parent_as, {parent_binding, parent_field}}, current_binding)
  end

  def build_query(query, schema_module, {:where, {schema_field, {operation, {:parent_as, params}}}}, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, schema_module, {:where, {schema_field, {operation, {:parent_as, {key, value}}}}}, current_binding)
    end)
  end

  def build_query(query, schema_module, {:where, {schema_field, {operation, values}}}, current_binding) when is_list(values) or is_map(values)  do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:where, {schema_field, {operation, value}}}, current_binding)
    end)
  end

  def build_query(query, _schema_module, {:where, {schema_field, {operation, value}}}, current_binding) do
    QueryExpression.where(query, schema_field, operation, value, current_binding)
  end

  def build_query(query, schema_module, {:where, {schema_field, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:where, {schema_field, value}}, current_binding)
    end)
  end

  def build_query(query, _schema_module, {:where, {schema_field, value}}, current_binding) do
    QueryExpression.where(query, schema_field, :==, value, current_binding)
  end

  def build_query(query, schema_module, {:where, values}, current_binding)  do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, {:where, value}, current_binding)
    end)
  end

  def build_query(query, schema_module, {key, value}, current_binding) do
    if association?(schema_module, key) do
      build_query(query, schema_module, {:join, {:association, {key, value}}}, current_binding)
    else
      build_query(query, schema_module, {:where, {key, value}}, current_binding)
    end
  end

  def build_query(query, schema_module, values, current_binding) do
    Enum.reduce(values, query, fn value, query ->
      build_query(query, schema_module, value, current_binding)
    end)
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
