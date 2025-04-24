defmodule EctoShorts.QueryBuilder.QueryExpressions do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Ecto Query expression builder API

  This module provides an api for composing ecto queries and building dynamic query expressions.
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder.Helpers,
    QueryBuilder.QueryAPI,
    QueryBuilder.QueryExpressions.Postgres
  }

  def join_association(query, current_binding, schema_module, key, params, fun) do
    assoc_schema_module = get_ecto_association_schema(schema_module, key)

    {assoc_binding, params} = Map.pop(params, :as)

    assoc_binding = assoc_binding || named_binding(key)

    query
    |> QueryAPI.join(
      {current_binding, assoc_binding},
      {:association, key, Map.take(params, [:on, :qualifier, :prefix])}
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
    |> QueryAPI.join(
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
    Helpers.apply_expressions(query, params, fn query, value ->
      QueryAPI.select(query, current_binding, value)
    end)
  end

  @doc """
  ...
  """
  def select_merge(query, current_binding, params) do
    Helpers.apply_expressions(query, params, fn query, value ->
      QueryAPI.select_merge(query, current_binding, value)
    end)
  end

  def or_where(query, current_binding, params) do
    Postgres.or_where(query, current_binding, params)
  end

  def or_where(query, current_binding, key, value) do
    Postgres.or_where(query, current_binding, key, value)
  end

  def or_where(query, current_binding, key, operator, value) do
    Postgres.or_where(query, current_binding, key, operator, value)
  end

  def where(query, current_binding, params) do
    Postgres.where(query, current_binding, params)
  end

  def where(query, current_binding, key, value) do
    Postgres.where(query, current_binding, key, value)
  end

  def where(query, current_binding, key, operator, value) do
    Postgres.where(query, current_binding, key, operator, value)
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
end
