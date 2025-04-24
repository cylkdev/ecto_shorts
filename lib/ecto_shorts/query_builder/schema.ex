defmodule EctoShorts.QueryBuilder.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.{
    QueryBuilder.Helpers,
    QueryBuilder.QueryExpressions
  }

  @behaviour EctoShorts.QueryBuilder

  @filters ~w(
    join
    select
    select_merge
    where
    or_where
  )a

  @doc """
  Returns a list of supported filters.
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
      query_expression_filter?(key) ->
        build_query_expression(query, current_binding, schema_module, key, value)

      association?(schema_module, key) ->
        build_assoc_filters(query, current_binding, schema_module, key, value)

      query_field?(schema_module, key) ->
        build_schema_filters(query, current_binding, schema_module, key, value)

      true ->
        EctoShorts.Utils.Logger.warning(
          __MODULE__,
          "Expected key to be a query field or association for the schema '#{inspect(schema_module)}', got: #{inspect(key)}"
        )

        query
    end
  end

  defp build_schema_filters(query, current_binding, schema_module, key, value) do
    Helpers.apply_expressions(query, value, fn query, value ->
      apply_schema_filter(query, current_binding, schema_module, key, value)
    end)
  end

  defp apply_schema_filter(query, current_binding, _schema_module, key, {operator, value}) do
    QueryExpressions.where(query, current_binding, key, operator, value)
  end

  defp apply_schema_filter(query, current_binding, _schema_module, key, value) do
    QueryExpressions.where(query, current_binding, key, :==, value)
  end

  defp build_query_expression(query, current_binding, schema_module, :join, value) do
    case value do
      {:association, params} ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_assoc_filters(query, current_binding, schema_module, key, value)
        end)

      {:subquery, params} ->
        build_subquery_filters(query, current_binding, schema_module, params)

      params ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_query_expression(query, current_binding, schema_module, :join, {key, value})
        end)
    end
  end

  defp build_query_expression(query, current_binding, _schema_module, :or_where, value) do
    QueryExpressions.or_where(query, current_binding, value)
  end

  defp build_query_expression(query, current_binding, _schema_module, :select, value) do
    QueryExpressions.select(query, current_binding, value)
  end

  defp build_query_expression(query, current_binding, _schema_module, :select_merge, value) do
    QueryExpressions.select_merge(query, current_binding, value)
  end

  defp build_assoc_filters(query, current_binding, schema_module, key, params) do
    QueryExpressions.join_association(
      query,
      current_binding,
      schema_module,
      key,
      params,
      fn query, assoc_binding, assoc_schema_module, params ->
        build_query(query, assoc_binding, assoc_schema_module, params)
      end
    )
  end

  defp build_subquery_filters(
         query,
         current_binding,
         schema_module,
         %{from: from} = params
       ) do
    QueryExpressions.join_subquery(
      query,
      current_binding,
      schema_module,
      from,
      params,
      fn query, subquery_binding, subquery_schema_module, params ->
        build_query(query, subquery_binding, subquery_schema_module, params)
      end
    )
  end

  defp query_expression_filter?(key) do
    key in @filters
  end

  defp query_field?(schema_module, key) do
    key in schema_module.__schema__(:query_fields)
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
