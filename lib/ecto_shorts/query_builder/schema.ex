defmodule EctoShorts.QueryBuilder.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.QueryBuilder.Schema
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder.Helpers,
    QueryBuilder.QueryAPI,
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
    join_association(
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
    join_subquery(
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

  def join_association(query, current_binding, schema_module, key, params, fun) do
    assoc_schema_module = ecto_association_schema(schema_module, key)

    {assoc_binding, params} = Map.pop(params, :as)

    assoc_binding = assoc_binding || named_binding(key)

    query
    |> QueryAPI.join(
      {current_binding, assoc_binding},
      :association,
      key,
      Map.take(params, [:on, :qualifier, :prefix])
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
      :subquery,
      from,
      Map.take(params, [:on, :qualifier, :prefix])
    )
    |> fun.(
      subquery_binding,
      subquery_schema_module,
      Map.drop(params, [:on, :qualifier, :prefix])
    )
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

  defp ecto_association_schema(schema_module, key) do
    case schema_module.__schema__(:association, key) do
      %{through: [field1, field2]} ->
        schema_module
        |> ecto_association_schema(field1)
        |> ecto_association_schema(field2)

      %{related: related} ->
        related
    end
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
