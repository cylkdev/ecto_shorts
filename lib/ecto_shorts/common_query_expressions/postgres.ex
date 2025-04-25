defmodule EctoShorts.CommonQueryExpressions.Postgres do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.CommonQueryExpressions.Postgres

  Provides Postgres-specific query expression helpers for use in
  EctoShorts. This module includes functions that leverage Postgres
  features and operators to enable advanced filtering, searching, and
  dynamic query construction for Postgres-backed schemas.

  These helpers are used internally by the QueryBuilder to provide
  robust support for Postgres-specific queries, such as array
  operations, case-insensitive matching, and custom fragments.
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder,
    CommonQueryExpressions.API,
    CommonQueryExpressions.Postgres.Array,
    CommonQueryExpressions.Postgres.Field
  }

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type binding() :: atom() | nil

  @type params :: map() | keyword()

  @type key :: atom()

  @type value :: any()

  @type operator :: any()

  @doc """
  Adds an OR-based where clause to the query using a list or parameters.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.Postgres.or_where(query, :user, [name: "Alice", age: 30])
      #Ecto.Query<...>
  """
  @spec or_where(query() | queryable() | source_queryable(), binding() | nil, params()) ::
          query() | queryable()
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
      QueryBuilder.apply_expressions(
        nil,
        params,
        &build_where_expr(&1, current_binding, schema_module, &2)
      )

    API.or_where(query, nil, dyn_expr)
  end

  @doc """
  Adds an OR-based where clause for a single field and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.Postgres.or_where(query, :user, :name, "Alice")
      #Ecto.Query<...>
  """
  @spec or_where(query() | queryable() | source_queryable(), binding() | nil, key(), value()) ::
          query() | queryable()
  def or_where(query, current_binding, key, value) do
    or_where(query, current_binding, key, :==, value)
  end

  @doc """
  Adds an OR-based where clause for a single field, operator, and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.Postgres.or_where(query, :user, :age, :>=, 18)
      #Ecto.Query<...>
  """
  @spec or_where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          key(),
          operator(),
          value()
        ) :: query() | queryable()
  def or_where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr = build_where_expr(nil, current_binding, schema_module, key, operator, value)

    API.or_where(query, nil, dyn_expr)
  end

  @doc """
  Adds a WHERE clause to the query using a list or parameters.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.Postgres.where(query, :user, [name: "Alice", age: 30])
      #Ecto.Query<...>
  """
  @spec where(query() | queryable() | source_queryable(), binding() | nil, params()) ::
          query() | queryable()
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
      QueryBuilder.apply_expressions(
        nil,
        params,
        &build_where_expr(&1, current_binding, schema_module, &2)
      )

    API.where(query, nil, dyn_expr)
  end

  @doc """
  Adds a WHERE clause for a single field and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.Postgres.where(query, :user, :name, "Alice")
      #Ecto.Query<...>
  """
  @spec where(query() | queryable() | source_queryable(), binding() | nil, key(), value()) ::
          query() | queryable()
  def where(query, current_binding, key, value) do
    where(query, current_binding, key, :==, value)
  end

  @doc """
  Adds a WHERE clause for a single field, operator, and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.Postgres.where(query, :user, :age, :>=, 18)
      #Ecto.Query<...>
  """
  @spec where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          key(),
          operator(),
          value()
        ) :: query() | queryable()
  def where(query, current_binding, key, operator, value) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    dyn_expr = build_where_expr(nil, current_binding, schema_module, key, operator, value)

    API.where(query, nil, dyn_expr)
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
      API.merge_dynamic(dyn, Array.where(current_binding, key, operator, value))
    else
      API.merge_dynamic(dyn, Array.where(current_binding, value, operator, key))
    end
  end

  defp put_where_field_expr(dyn, current_binding, value, operator, key) do
    API.merge_dynamic(dyn, Field.where(current_binding, value, operator, key))
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
