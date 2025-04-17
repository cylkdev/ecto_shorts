defmodule EctoShorts.CommonFiltersRc.SchemaQueryBuilder do
  @moduledoc """

  * `since` - Records inserted on or after a time
  * `until` - Records inserted up to and including this time
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
    |> build_query_expression(schema_module, params, current_binding)
  end

  # common filters

  def build_query_expression(query, _schema_module, {:preload, expr}, current_binding) do
    expr = if is_map(expr), do: Map.to_list(expr), else: expr

    QueryExpression.preload(query, expr, current_binding)
  end

  def build_query_expression(query, _schema_module, {:after, value}, current_binding) do
    QueryExpression.where(query, :id, :>, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:before, value}, current_binding) do
    QueryExpression.where(query, :id, :<, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:since, value}, current_binding) do
    QueryExpression.where(query, :inserted_at, :>=, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:until, value}, current_binding) do
    QueryExpression.where(query, :inserted_at, :<=, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:start_date, value}, current_binding) do
    QueryExpression.where(query, :inserted_at, :>=, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:end_date, value}, current_binding) do
    QueryExpression.where(query, :inserted_at, :<=, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:ids, values}, current_binding) do
    QueryExpression.where(query, :id, :in, values, current_binding)
  end

  def build_query_expression(query, _schema_module, {:offset, value}, current_binding) do
    QueryExpression.offset(query, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:limit, value}, current_binding) do
    QueryExpression.limit(query, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:first, value}, current_binding) do
    QueryExpression.limit(query, value, current_binding)
  end

  def build_query_expression(query, _schema_module, {:last, value}, current_binding) do
    query
    |> QueryExpression.exclude(:order_by, nil)
    |> QueryExpression.from(order_by: [desc: :inserted_at, limit: value])
    |> QueryExpression.subquery([])
    |> QueryExpression.order_by(:id, current_binding)
  end

  def build_query_expression(query, schema_module, {:search, value}, current_binding) do
    cond do
      Code.ensure_loaded?(schema_module) and function_exported?(schema_module, :by_search, 3) ->
        schema_module.by_search(query, value, current_binding)

      Code.ensure_loaded?(schema_module) and function_exported?(schema_module, :by_search, 2) ->
        schema_module.by_search(query, value)

      true ->
        EctoShorts.Utils.Logger.warning(__MODULE__, "The schema module #{inspect(schema_module)} does not export the function `by_search/2`.")

        query
    end
  end

  # with_named_binding

  def build_query_expression(query, schema_module, {key, {:with_named_binding, params}}, _current_binding) do
    Enum.reduce(params, query, fn {binding_alias, value}, query ->
      build_query_expression(query, schema_module, {key, value}, binding_alias)
    end)
  end

  def build_query_expression(query, schema_module, {:with_named_binding, params}, _current_binding) do
    Enum.reduce(params, query, fn {binding_alias, value}, query ->
      build_query_expression(query, schema_module, value, binding_alias)
    end)
  end

  # join

  def build_query_expression(query, schema_module, {:join, {:association, {assoc_key, params}}}, current_binding) do
    filter_params = Map.drop(params, [:as, :with_named_binding, :on, :prefix, :qualifier])

    context = Map.take(params, [:as, :with_named_binding, :on, :prefix, :qualifier])

    current_binding = context[:with_named_binding] || current_binding

    join_qual = context[:qualifier] || :left

    join_prefix = context[:prefix]

    join_binding = with nil <- context[:as], do: QueryExpression.named_binding_atom(assoc_key)

    join_on =
      case context[:on] do
        params when is_list(params) or is_map(params) -> QueryExpression.dynamic(params, join_binding)
        _ -> true
      end

    ecto_assoc = schema_module.__schema__(:association, assoc_key)

    assoc_schema_module = ecto_assoc.queryable

    query
    |> QueryExpression.join(:association, assoc_key, join_qual, join_on, join_prefix, join_binding, current_binding)
    |> build_query_expression(assoc_schema_module, filter_params, join_binding)
  end

  def build_query_expression(query, schema_module, {:join, {:association, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:join, {:association, value}}, current_binding)
    end)
  end

  def build_query_expression(query, schema_module, {:join, {:association, key}}, current_binding) do
    build_query_expression(query, schema_module, {:join, {:association, {key, %{}}}}, current_binding)
  end

  def build_query_expression(query, _schema_module, {:join, {:subquery, params}}, current_binding) when is_map(params) do
    filter_params = Map.drop(params, [:from, :as, :with_named_binding, :on, :prefix, :qualifier])

    context = Map.take(params, [:from, :as, :with_named_binding, :on, :prefix, :qualifier])

    current_binding = context[:with_named_binding] || current_binding

    subquery_from = context.from

    subquery_schema_module = CommonSchemas.get_schema_queryable(subquery_from)

    join_qual = context[:qualifier] || :left

    join_prefix = context[:prefix]

    join_binding =
      with nil <- context[:as] do
        subquery_schema_module
        |> Module.split()
        |> List.last()
        |> Macro.underscore()
        |> QueryExpression.named_binding_atom()
      end

    join_on =
      case context[:on] do
        params when is_list(params) or is_map(params) -> QueryExpression.dynamic(params, join_binding)
        _ -> true
      end

    query
    |> QueryExpression.join(:subquery, subquery_from, join_qual, join_on, join_prefix, join_binding, current_binding)
    |> build_query_expression(subquery_schema_module, filter_params, join_binding)
  end

  def build_query_expression(query, schema_module, {:join, {:subquery, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:join, {:subquery, value}}, current_binding)
    end)
  end

  def build_query_expression(query, schema_module, {:join, {:subquery, query}}, current_binding) do
    build_query_expression(query, schema_module, {:join, {:subquery, %{query: query}}}, current_binding)
  end

  def build_query_expression(query, schema_module, {:join, values}, current_binding) do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:join, value}, current_binding)
    end)
  end

  # select

  def build_query_expression(query, _schema_module, {:select, {type, keys}}, current_binding) do
    QueryExpression.select(query, type, keys, current_binding)
  end

  def build_query_expression(query, _schema_module, {:select, true}, current_binding) do
    QueryExpression.select(query, current_binding)
  end

  def build_query_expression(query, schema_module, {:select, values}, current_binding) when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.reduce(values, query, fn {key, value}, query ->
        build_query_expression(query, schema_module, {:select, {key, value}}, current_binding)
      end)
    else
      QueryExpression.select(query, :map, values, current_binding)
    end
  end

  def build_query_expression(query, schema_module, {:select, params}, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query_expression(query, schema_module, {:select, {key, value}}, current_binding)
    end)
  end

  # select_merge

  def build_query_expression(query, _schema_module, {:select_merge, true}, current_binding) do
    QueryExpression.select_merge(query, current_binding)
  end

  def build_query_expression(query, _schema_module, {:select_merge, keys}, current_binding) do
    QueryExpression.select_merge(query, keys, current_binding)
  end

  # or_where

  def build_query_expression(query, _schema_module, {:or_where, {schema_field, {operation, {:parent_as, {parent_binding, parent_field}}}}}, current_binding) do
    QueryExpression.or_where(query, schema_field, operation, {:parent_as, {parent_binding, parent_field}}, current_binding)
  end

  def build_query_expression(query, schema_module, {:or_where, {schema_field, {operation, {:parent_as, params}}}}, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query_expression(query, schema_module, {:or_where, {schema_field, {operation, {:parent_as, {key, value}}}}}, current_binding)
    end)
  end

  def build_query_expression(query, schema_module, {:or_where, {schema_field, {operation, values}}}, current_binding) when is_list(values) or is_map(values)  do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:or_where, {schema_field, {operation, value}}}, current_binding)
    end)
  end

  def build_query_expression(query, _schema_module, {:or_where, {schema_field, {operation, value}}}, current_binding) do
    QueryExpression.or_where(query, schema_field, operation, value, current_binding)
  end

  def build_query_expression(query, schema_module, {:or_where, {schema_field, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:or_where, {schema_field, value}}, current_binding)
    end)
  end

  def build_query_expression(query, _schema_module, {:or_where, {schema_field, value}}, current_binding) do
    QueryExpression.or_where(query, schema_field, :==, value, current_binding)
  end

  def build_query_expression(query, schema_module, {:or_where, values}, current_binding)  do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:or_where, value}, current_binding)
    end)
  end

  # where

  def build_query_expression(query, _schema_module, {:where, {schema_field, {operation, {:parent_as, {parent_binding, parent_field}}}}}, current_binding) do
    QueryExpression.where(query, schema_field, operation, {:parent_as, {parent_binding, parent_field}}, current_binding)
  end

  def build_query_expression(query, schema_module, {:where, {schema_field, {operation, {:parent_as, params}}}}, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query_expression(query, schema_module, {:where, {schema_field, {operation, {:parent_as, {key, value}}}}}, current_binding)
    end)
  end

  def build_query_expression(query, schema_module, {:where, {schema_field, {operation, values}}}, current_binding) when is_list(values) or is_map(values)  do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:where, {schema_field, {operation, value}}}, current_binding)
    end)
  end

  def build_query_expression(query, _schema_module, {:where, {schema_field, {operation, value}}}, current_binding) do
    QueryExpression.where(query, schema_field, operation, value, current_binding)
  end

  def build_query_expression(query, schema_module, {:where, {schema_field, values}}, current_binding) when is_list(values) or is_map(values) do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:where, {schema_field, value}}, current_binding)
    end)
  end

  def build_query_expression(query, _schema_module, {:where, {schema_field, value}}, current_binding) do
    QueryExpression.where(query, schema_field, :==, value, current_binding)
  end

  def build_query_expression(query, schema_module, {:where, values}, current_binding)  do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, {:where, value}, current_binding)
    end)
  end

  def build_query_expression(query, schema_module, {key, value}, current_binding) do
    if association?(schema_module, key) do
      build_query_expression(query, schema_module, {:join, {:association, {key, value}}}, current_binding)
    else
      build_query_expression(query, schema_module, {:where, {key, value}}, current_binding)
    end
  end

  def build_query_expression(query, schema_module, values, current_binding) do
    Enum.reduce(values, query, fn value, query ->
      build_query_expression(query, schema_module, value, current_binding)
    end)
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
