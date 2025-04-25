defmodule EctoShorts.QueryBuilder.QueryExpression do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.QueryBuilder.QueryExpression

  Provides a set of functions for composing Ecto queries and
  building dynamic query expressions in a flexible, programmatic way.

  This module defines the high-level API for constructing database
  specific ecto query expressions.
  """

  alias EctoShorts.{
    QueryBuilder.Helpers,
    QueryBuilder.QueryExpression.API,
    QueryBuilder.QueryExpression.Postgres
  }

  @doc """
  ...
  """
  defdelegate merge_dynamic(dyn_a, dyn_b), to: API

  @doc """
  ...
  """
  defdelegate dynamic(current_binding \\ nil, value), to: API

  @doc """
  ...
  """
  defdelegate from(query, opts \\ []), to: API

  @doc """
  ...
  """
  defdelegate put_query_prefix(query, prefix), to: API

  @doc """
  ...
  """
  defdelegate subquery(query, opts \\ []), to: API

  @doc """
  ...
  """
  defdelegate join(query, current_binding, type, key, opts \\ []), to: API

  @doc """
  ...
  """
  defdelegate exclude(query, field), to: API

  @doc """
  ...
  """
  defdelegate group_by(query, current_binding, value), to: API

  @doc """
  ...
  """
  defdelegate order_by(query, current_binding, value), to: API

  @doc """
  ...
  """
  defdelegate limit(query, current_binding, value), to: API

  @doc """
  ...
  """
  defdelegate offset(query, current_binding, value), to: API

  @doc """
  ...
  """
  defdelegate preload(query, current_binding, value), to: API

  @doc """
  Builds a `select` expression for an Ecto query based on the provided parameters.

  Recursively traverses the `params` structure (which can be a map, keyword list, or list of fields) and applies each field or nested field to the query using `QueryExpression.API.select/3`.

  This enables dynamic selection of fields, including deeply nested associations, in a composable way.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the select expression.
    - params: A map, keyword list, or list describing the fields/associations to select.

  ## Example

      params = %{user: %{name: true, profile: %{age: true}}}
      select(query, :user, params)

  Returns the modified query with the select expression applied.
  """
  def select(query, current_binding, value) do
    Helpers.apply_expressions(query, value, fn query, value ->
      API.select(query, current_binding, value)
    end)
  end

  @doc """
  Builds a `select_merge` expression for an Ecto query based on the provided parameters.

  Recursively traverses the `params` structure and merges the selected fields or associations into the query using `QueryExpression.API.select_merge/3`.

  Useful for cases where you want to add or merge fields into an existing select expression, including nested fields.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the select_merge expression.
    - params: A map, keyword list, or list describing the fields/associations to merge into the select.

  ## Example

      params = %{settings: %{theme: true, notifications: true}}
      select_merge(query, :settings, params)

  Returns the modified query with the select_merge expression applied.
  """
  def select_merge(query, current_binding, value) do
    Helpers.apply_expressions(query, value, fn query, value ->
      API.select_merge(query, current_binding, value)
    end)
  end

  @doc """
  Builds an `or_where` expression for an Ecto query based on the provided parameters.

  Delegates to `Postgres.or_where/3` to construct a dynamic OR-based where clause from the given parameters. The parameters can be a map, keyword list, or other supported structure, allowing for flexible and composable logical OR filters.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the where clause.
    - params: A map, keyword list, or list describing the OR conditions to apply.

  ## Example

      params = %{role: "admin", active: true}
      or_where(query, :user, params)

  Returns the modified query with the OR-based where clause applied.
  """
  def or_where(query, current_binding, value) do
    Postgres.or_where(query, current_binding, value)
  end

  @doc """
  Builds an `or_where` expression for an Ecto query using a single key and value.

  Delegates to `Postgres.or_where/4` to construct a dynamic OR-based where clause for a single field and value.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the where clause.
    - key: The field or association key to filter on.
    - value: The value to match for the given key.

  ## Example

      or_where(query, :user, :role, "admin")

  Returns the modified query with the OR-based where clause applied for the given key and value.
  """
  def or_where(query, current_binding, key, value) do
    Postgres.or_where(query, current_binding, key, value)
  end

  @doc """
  Builds an `or_where` expression for an Ecto query using a single key, operator, and value.

  Delegates to `Postgres.or_where/5` to construct a dynamic OR-based where clause for a single field, operator, and value.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the where clause.
    - key: The field or association key to filter on.
    - operator: The operator to use for the filter (e.g. `:==`, `:!=`, `:>`, etc.).
    - value: The value to match for the given key and operator.

  ## Example

      or_where(query, :user, :role, :==, "admin")

  Returns the modified query with the OR-based where clause applied for the given key, operator, and value.
  """
  def or_where(query, current_binding, key, operator, value) do
    Postgres.or_where(query, current_binding, key, operator, value)
  end

  @doc """
  Builds a `where` expression for an Ecto query based on the provided parameters.

  Delegates to `Postgres.where/3` to construct a dynamic where clause from the given parameters. The parameters can be a map, keyword list, or other supported structure, allowing for flexible and composable filters.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the where clause.
    - params: A map, keyword list, or list describing the conditions to apply.

  ## Example

      params = %{role: "admin", active: true}
      where(query, :user, params)

  Returns the modified query with the where clause applied.
  """
  def where(query, current_binding, value) do
    Postgres.where(query, current_binding, value)
  end

  @doc """
  Builds a `where` expression for an Ecto query using a single key and value.

  Delegates to `Postgres.where/4` to construct a dynamic where clause for a single field and value.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the where clause.
    - key: The field or association key to filter on.
    - value: The value to match for the given key.

  ## Example

      where(query, :user, :role, "admin")

  Returns the modified query with the where clause applied for the given key and value.
  """
  def where(query, current_binding, key, value) do
    Postgres.where(query, current_binding, key, value)
  end

  @doc """
  Builds a `where` expression for an Ecto query using a single key, operator, and value.

  Delegates to `Postgres.where/5` to construct a dynamic where clause for a single field, operator, and value.

  ## Parameters

    - query: The Ecto queryable to modify.
    - current_binding: The current binding (atom or index) for the where clause.
    - key: The field or association key to filter on.
    - operator: The operator to use for the filter (e.g. `:==`, `:!=`, `:>`, etc.).
    - value: The value to match for the given key and operator.

  ## Example

      where(query, :user, :role, :==, "admin")

  Returns the modified query with the where clause applied for the given key, operator, and value.
  """
  def where(query, current_binding, key, operator, value) do
    Postgres.where(query, current_binding, key, operator, value)
  end
end
