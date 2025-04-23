defmodule EctoShorts.QueryBuilders.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilders.ExpressionBuilder,
    QueryBuilders.Expression
  }

  @behaviour EctoShorts.QueryBuilder

  @filters ~w(
    join
    select
    select_merge
    where
    or
    or_where
  )a

  @doc """
  ...
  """
  def filters, do: @filters

  def build_query(query, current_binding, schema_module, params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, current_binding, schema_module, key, value)
    end)
  end

  @impl EctoShorts.QueryBuilder
  @doc """
  ...
  """
  def build_query(query, current_binding, schema_module, key, value) do
    cond do
      query_filter?(key) ->
        build_query_expression(query, current_binding, schema_module, key, value)

      association?(schema_module, key) ->
        build_assoc_expressions(query, current_binding, schema_module, key, value)

      query_field?(schema_module, key) ->
        build_schema_expressions(query, current_binding, schema_module, key, value)

      true ->
        EctoShorts.Utils.Logger.warning(
          __MODULE__,
          "Expected key to be a query field or association for the schema '#{inspect(schema_module)}', got: #{inspect(key)}"
        )

        query
    end
  end

  defp build_schema_expressions(query, current_binding, schema_module, key, value) do
    ExpressionBuilder.apply_expressions(query, value, fn query, value ->
      apply_schema_expression(query, current_binding, schema_module, key, value)
    end)
  end

  defp apply_schema_expression(query, current_binding, schema_module, key, {operator, value}) do
    source_key = field_source(schema_module, key)

    ExpressionBuilder.where(query, current_binding, source_key, operator, value)
  end

  defp apply_schema_expression(query, current_binding, schema_module, key, value) do
    source_key = field_source(schema_module, key)

    ExpressionBuilder.where(query, current_binding, source_key, :==, value)
  end

  defp build_assoc_expressions(query, current_binding, schema_module, key, value) do
    build_association_expression(query, current_binding, schema_module, key, value)
  end

  defp build_query_expression(query, current_binding, schema_module, :join, value) do
    case value do
      {:association, params} ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_association_expression(query, current_binding, schema_module, key, value)
        end)

      {:subquery, params} ->
        build_subquery_expression(query, current_binding, schema_module, params)

      params ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_query_expression(query, current_binding, schema_module, :join, {key, value})
        end)
    end
  end

  defp build_query_expression(query, current_binding, schema_module, operator, value)
       when operator in [:or, :or_where] do
    ExpressionBuilder.apply_expressions(query, value, fn query, value ->
      or_where(query, current_binding, schema_module, value)
    end)
  end

  defp build_query_expression(query, current_binding, _schema_module, :select, value) do
    ExpressionBuilder.select(query, current_binding, value)
  end

  defp build_query_expression(query, current_binding, _schema_module, :select_merge, value) do
    ExpressionBuilder.select_merge(query, current_binding, value)
  end

  defp or_where(query, current_binding, schema_module, {key, {operator, value}}) do
    source_key = field_source(schema_module, key)

    ExpressionBuilder.or_where(query, current_binding, source_key, operator, value)
  end

  defp or_where(query, current_binding, schema_module, {key, value}) do
    source_key = field_source(schema_module, key)

    ExpressionBuilder.or_where(query, current_binding, source_key, :==, value)
  end

  defp build_association_expression(query, current_binding, schema_module, key, params) do
    source_key = field_source(schema_module, key)

    {as, params} = Map.pop(params, :as)

    assoc_binding = as || named_binding(source_key)

    ExpressionBuilder.join_association(
      query,
      {current_binding, assoc_binding},
      schema_module,
      source_key,
      params,
      fn query, assoc_binding, assoc_schema_module, params ->
        build_query(query, assoc_binding, assoc_schema_module, params)
      end
    )
  end

  defp build_subquery_expression(
         query,
         current_binding,
         _schema_module,
         %{from: from} = params
       ) do
    {as, join_params} = Map.pop(params, :as)

    subquery_schema_module = CommonSchemas.get_schema_queryable(from)

    subquery_binding =
      with nil <- as do
        named_binding_from_module(subquery_schema_module)
      end

    params = Map.drop(params, [:on, :qualifier, :prefix])

    query
    |> Expression.join({current_binding, subquery_binding}, :subquery, from, join_params)
    |> build_query(subquery_binding, subquery_schema_module, params)
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

  defp field_source(schema_module, key) do
    schema_module.__schema__(:field_source, key) || key
  end

  defp query_filter?(key) do
    key in @filters
  end

  defp query_field?(schema_module, key) do
    key in schema_module.__schema__(:query_fields)
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
