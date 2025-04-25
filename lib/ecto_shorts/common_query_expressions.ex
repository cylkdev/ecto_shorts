defmodule EctoShorts.CommonQueryExpressions do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.CommonQueryExpressions

  This module defines functions for building dynamic Ecto queries,
  supporting a wide range of filters, ordering, and logical
  expressions.

  These are used to enable flexible, composable, and efficient query
  construction across supported databases.
  """

  alias EctoShorts.{
    QueryBuilder,
    CommonQueryExpressions.API,
    CommonQueryExpressions.Postgres
  }

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type dynamic_expr :: Ecto.Query.DynamicExpr.t()

  @type limit :: integer()

  @type offset :: integer()

  @type join_type :: :association | :subquery

  @type binding :: atom() | nil

  @type prefix :: binary()

  @type field :: atom()

  @type key :: atom()

  @type operator :: atom()

  @type value :: any()

  @type opts :: keyword()

  @doc """
  Merges two dynamic query expressions using logical `and`.
  If the first argument is `nil`, returns the second.

  Delegates to `EctoShorts.CommonQueryExpressions.merge_dynamic`.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.merge_dynamic(nil, dynamic([q], q.id > 1))
      #Ecto.Query.DynamicExpr<...>

      iex> EctoShorts.CommonQueryExpressions.merge_dynamic(dynamic([q], q.id > 1), dynamic([q], q.name == "foo"))
      #Ecto.Query.DynamicExpr<...>
  """
  @spec merge_dynamic(dynamic_expr() | nil, dynamic_expr()) :: dynamic_expr()
  defdelegate merge_dynamic(dyn_a, dyn_b), to: API

  @doc """
  Builds a dynamic query expression for use in Ecto queries.

  Delegates to `EctoShorts.CommonQueryExpressions.dynamic/2`.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.dynamic(:user, user.age > 18)
      #Ecto.Query.DynamicExpr<...>

      iex> EctoShorts.CommonQueryExpressions.dynamic(nil, q.id == 1)
      #Ecto.Query.DynamicExpr<...>
  """
  @spec dynamic(binding(), value()) :: dynamic_expr()
  defdelegate dynamic(current_binding \\ nil, value), to: API

  @doc """
  Sets the `from` clause on a query.

  Delegates to `EctoShorts.CommonQueryExpressions.from/2`.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.from(query, params)
      #Ecto.Query<...>
  """
  @spec from(query() | queryable() | source_queryable(), opts()) :: query()
  defdelegate from(query, opts \\ []), to: API

  @doc """
  Sets the prefix for the query (e.g., for multi-tenancy).

  Delegates to `EctoShorts.CommonQueryExpressions.put_query_prefix/2`.
  """
  @spec put_query_prefix(query() | queryable() | source_queryable(), prefix()) ::
          query()
  defdelegate put_query_prefix(query, prefix), to: API

  @doc """
  Creates a subquery from the given queryable source.

  Delegates to `EctoShorts.CommonQueryExpressions.subquery/2`.
  """
  @spec subquery(query() | queryable() | source_queryable(), opts()) :: query()
  defdelegate subquery(query, opts \\ []), to: API

  @doc """
  Performs a join on the query using the provided binding, type, key, and params.

  Delegates to `EctoShorts.CommonQueryExpressions.join/5`.
  """
  @spec join(
          query() | queryable() | source_queryable(),
          binding() | {binding(), binding()},
          join_type(),
          key(),
          opts()
        ) :: query()
  defdelegate join(query, current_binding, join_type, key, opts \\ []), to: API

  @doc """
  Excludes a field from the query (e.g., select, where, etc.).

  Delegates to `EctoShorts.CommonQueryExpressions.exclude/2`.
  """
  @spec exclude(query() | queryable() | source_queryable(), field()) :: query()
  defdelegate exclude(query, field), to: API

  @doc """
  Adds a `group_by` clause to the query for the given binding and value.

  Delegates to `EctoShorts.CommonQueryExpressions.group_by/3`.
  """
  @spec group_by(query() | queryable() | source_queryable(), binding() | nil, value()) ::
          query()
  defdelegate group_by(query, current_binding, value), to: API

  @doc """
  Adds an `order_by` clause to the query for the given binding and value.

  Delegates to `EctoShorts.CommonQueryExpressions.order_by/3`.
  """
  @spec order_by(query() | queryable() | source_queryable(), binding() | nil, value()) ::
          query()
  defdelegate order_by(query, current_binding, value), to: API

  @doc """
  Sets a `limit` on the number of results for the query.

  Delegates to `EctoShorts.CommonQueryExpressions.limit/3`.
  """
  @spec limit(query() | queryable() | source_queryable(), binding() | nil, limit()) ::
          query()
  defdelegate limit(query, current_binding, value), to: API

  @doc """
  Sets an `offset` for the query results.

  Delegates to `EctoShorts.CommonQueryExpressions.offset/3`.
  """
  @spec offset(query() | queryable() | source_queryable(), binding() | nil, offset()) ::
          query()
  defdelegate offset(query, current_binding, value), to: API

  @doc """
  Adds a preload for the given binding and value.

  Delegates to `EctoShorts.CommonQueryExpressions.preload/3`.
  """
  @spec preload(query() | queryable() | source_queryable(), binding() | nil, value()) ::
          query()
  defdelegate preload(query, current_binding, value), to: API

  @doc """
  Builds a `select` expression for an Ecto query based on the provided parameters.

  Recursively traverses the `params` structure (which can be a map, keyword list, or list of fields) and applies each field or nested field to the query using `CommonQueryExpressions.select/3`.

  This enables dynamic selection of fields, including deeply nested associations, in a composable way.

  ## Example

      iex> params = %{user: %{name: true, profile: %{age: true}}}
      ...> EctoShorts.CommonQueryExpressions.select(query, :user, params)
  """
  @spec select(query() | queryable() | source_queryable(), binding() | nil, value()) ::
          query()
  def select(query, current_binding, value) do
    QueryBuilder.apply_expressions(query, value, fn query, value ->
      API.select(query, current_binding, value)
    end)
  end

  @doc """
  Builds a `select_merge` expression for an Ecto query based on the provided parameters.

  Recursively traverses the `params` structure and merges the selected fields or associations into the query using `CommonQueryExpressions.select_merge/3`.

  Useful for cases where you want to add or merge fields into an existing select expression, including nested fields.

  ## Example

      iex> params = %{settings: %{theme: true, notifications: true}}
      ...> EctoShorts.CommonQueryExpressions.select_merge(query, :settings, params)
  """
  @spec select_merge(query() | queryable() | source_queryable(), binding() | nil, value()) ::
          query()
  def select_merge(query, current_binding, value) do
    QueryBuilder.apply_expressions(query, value, fn query, value ->
      API.select_merge(query, current_binding, value)
    end)
  end

  @doc """
  Builds an `or_where` expression for an Ecto query based on the provided parameters.

  Delegates to `Postgres.or_where/3` to construct a dynamic OR-based where clause from the given parameters. The parameters can be a map, keyword list, or other supported structure, allowing for flexible and composable logical OR filters.

  ## Example

      iex> params = %{role: "admin", active: true}
      ...> EctoShorts.CommonQueryExpressions.or_where(query, :user, params)
  """
  @spec or_where(query() | queryable() | source_queryable(), binding() | nil, value()) ::
          query()
  def or_where(query, current_binding, value) do
    Postgres.or_where(query, current_binding, value)
  end

  @doc """
  Builds an `or_where` expression for an Ecto query using a single key and value.

  Delegates to `Postgres.or_where/4` to construct a dynamic OR-based where clause for a single field and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.or_where(query, :user, :role, "admin")
  """
  @spec or_where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          key(),
          value()
        ) ::
          query()
  def or_where(query, current_binding, key, value) do
    Postgres.or_where(query, current_binding, key, value)
  end

  @doc """
  Builds an `or_where` expression for an Ecto query using a single key, operator, and value.

  Delegates to `Postgres.or_where/5` to construct a dynamic OR-based where clause for a single field, operator, and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.or_where(query, :user, :role, :==, "admin")
  """
  @spec or_where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          key(),
          operator(),
          value()
        ) :: query()
  def or_where(query, current_binding, key, operator, value) do
    Postgres.or_where(query, current_binding, key, operator, value)
  end

  @doc """
  Builds a `where` expression for an Ecto query based on the provided parameters.

  Delegates to `Postgres.where/3` to construct a dynamic where clause from the given parameters. The parameters can be a map, keyword list, or other supported structure, allowing for flexible and composable filters.

  ## Example

      iex> params = %{role: "admin", active: true}
      ...> EctoShorts.CommonQueryExpressions.where(query, :user, params)
  """
  @spec where(query() | queryable() | source_queryable(), binding() | nil, value()) :: query()
  def where(query, current_binding, value) do
    Postgres.where(query, current_binding, value)
  end

  @doc """
  Builds a `where` expression for an Ecto query using a single key and value.

  Delegates to `Postgres.where/4` to construct a dynamic where clause for a single field and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.where(query, :user, :role, "admin")
  """
  @spec where(query() | queryable() | source_queryable(), binding() | nil, key(), value()) ::
          query()
  def where(query, current_binding, key, value) do
    Postgres.where(query, current_binding, key, value)
  end

  @doc """
  Builds a `where` expression for an Ecto query using a single key, operator, and value.

  Delegates to `Postgres.where/5` to construct a dynamic where clause for a single field, operator, and value.

  ## Example

      iex> EctoShorts.CommonQueryExpressions.where(query, :user, :role, :==, "admin")
  """
  @spec where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          key(),
          operator(),
          value()
        ) :: query()
  def where(query, current_binding, key, operator, value) do
    Postgres.where(query, current_binding, key, operator, value)
  end
end
