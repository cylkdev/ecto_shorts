defmodule EctoShorts.QueryBuilders.Schema do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilders.DynamicExpression,
    QueryBuilders.Expression
  }

  @behaviour EctoShorts.QueryBuilder

  @filters ~w(
    join
    select
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
        build_query_filter(query, current_binding, schema_module, key, value)

      association?(schema_module, key) ->
        build_assoc_filter(query, current_binding, schema_module, key, value)

      query_field?(schema_module, key) ->
        build_schema_filter(query, current_binding, schema_module, key, value)

      true ->
        EctoShorts.Utils.Logger.warning(
          __MODULE__,
          "Expected key to be a query field or association for the schema '#{inspect(schema_module)}', got: #{inspect(key)}"
        )

        query
    end
  end

  defp build_schema_filter(query, current_binding, schema_module, key, value) do
    DynamicExpression.apply_filters(query, value, fn query, value ->
      apply_dynamic_schema_filter(query, current_binding, schema_module, key, value)
    end)
  end

  defp apply_dynamic_schema_filter(query, current_binding, schema_module, key, {operator, value}) do
    source_key = field_source(schema_module, key)

    DynamicExpression.where(query, current_binding, source_key, operator, value)
  end

  defp apply_dynamic_schema_filter(query, current_binding, schema_module, key, value) do
    apply_dynamic_schema_filter(query, current_binding, schema_module, key, {:==, value})
  end

  defp build_assoc_filter(query, current_binding, schema_module, key, value) do
    join_association(query, current_binding, schema_module, key, value)
  end

  defp build_query_filter(query, current_binding, schema_module, :join, value) do
    case value do
      {:association, params} ->
        Enum.reduce(params, query, fn {key, value}, query ->
          join_association(query, current_binding, schema_module, key, value)
        end)

      {:subquery, params} ->
        join_subquery(query, current_binding, schema_module, params)

      params ->
        Enum.reduce(params, query, fn {key, value}, query ->
          build_query_filter(query, current_binding, schema_module, :join, {key, value})
        end)
    end
  end

  defp build_query_filter(query, current_binding, schema_module, operator, value)
       when operator in [:or, :or_where] do
    DynamicExpression.apply_filters(query, value, fn query, value ->
      or_where(query, current_binding, schema_module, value)
    end)
  end

  defp build_query_filter(query, current_binding, _schema_module, :select, value) do
    Expression.select(query, current_binding, value)
  end

  defp or_where(query, current_binding, schema_module, {key, {operator, value}}) do
    source_key = field_source(schema_module, key)

    DynamicExpression.or_where(query, current_binding, source_key, operator, value)
  end

  defp or_where(query, current_binding, schema_module, {key, value}) do
    or_where(query, current_binding, schema_module, {key, {:==, value}})
  end

  defp join_association(query, current_binding, schema_module, key, params) do
    source_key = field_source(schema_module, key)

    assoc_schema_module = ecto_association_schema(schema_module, source_key)

    {as, join_params} = Map.pop(params, :as)

    assoc_binding = as || named_binding(source_key)

    params = Map.drop(params, [:on, :qualifier, :prefix])

    query
    |> Expression.join({current_binding, assoc_binding}, :association, source_key, join_params)
    |> build_query(assoc_binding, assoc_schema_module, params)
  end

  defp join_subquery(query, current_binding, _schema_module, %{from: from} = params) do
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
