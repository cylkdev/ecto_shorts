defmodule EctoShorts.QueryBuilder.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.QueryBuilder.ExpressionBuilder

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
      api_filter?(key) ->
        build_api_filters(query, current_binding, schema_module, key, value)

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
    ExpressionBuilder.apply_expressions(query, value, fn query, value ->
      apply_schema_filter(query, current_binding, schema_module, key, value)
    end)
  end

  defp apply_schema_filter(query, current_binding, _schema_module, key, {operator, value}) do
    ExpressionBuilder.where(query, current_binding, key, operator, value)
  end

  defp apply_schema_filter(query, current_binding, _schema_module, key, value) do
    ExpressionBuilder.where(query, current_binding, key, :==, value)
  end

  defp build_api_filters(query, current_binding, schema_module, :join, value) do
    case value do
      {:association, params} ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_assoc_filters(query, current_binding, schema_module, key, value)
        end)

      {:subquery, params} ->
        build_subquery_filters(query, current_binding, schema_module, params)

      params ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_api_filters(query, current_binding, schema_module, :join, {key, value})
        end)
    end
  end

  defp build_api_filters(query, current_binding, schema_module, operator, value)
       when operator in [:or, :or_where] do
    ExpressionBuilder.apply_expressions(query, value, fn query, value ->
      or_where(query, current_binding, schema_module, value)
    end)
  end

  defp build_api_filters(query, current_binding, _schema_module, :select, value) do
    ExpressionBuilder.select(query, current_binding, value)
  end

  defp build_api_filters(query, current_binding, _schema_module, :select_merge, value) do
    ExpressionBuilder.select_merge(query, current_binding, value)
  end

  defp or_where(query, current_binding, _schema_module, {key, {operator, value}}) do
    ExpressionBuilder.or_where(query, current_binding, key, operator, value)
  end

  defp or_where(query, current_binding, _schema_module, {key, value}) do
    ExpressionBuilder.or_where(query, current_binding, key, :==, value)
  end

  defp build_assoc_filters(query, current_binding, schema_module, key, params) do
    ExpressionBuilder.join_association(
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
    ExpressionBuilder.join_subquery(
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

  defp api_filter?(key) do
    key in @filters
  end

  defp query_field?(schema_module, key) do
    key in schema_module.__schema__(:query_fields)
  end

  defp association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end
end
