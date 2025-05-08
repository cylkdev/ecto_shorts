defmodule EctoShorts.QueryBuilders.Common do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides common query-building functionality for `EctoShorts`.

  This module implements the `EctoShorts.QueryBuilder` behaviour for filters
  that are common across many schemas. These include pagination, ordering,
  ID filtering, and temporal filters.

  It is designed to be composable and reusable, working alongside
  schema-specific query builders such as `EctoShorts.QueryBuilders.Schema`.

  ## Examples

      iex> EctoShorts.QueryBuilders.Common.filters()
      [:after, :before, :end_date, :first, :ids, :last, :limit, :offset, :order_by, :preload, :search, :since, :start_date, :until]

      iex> EctoShorts.QueryBuilders.Common.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :limit, 10)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, limit: ^10>
  """

  alias EctoShorts.CommonQuery

  @filters ~w(
    after
    before
    end_date
    first
    ids
    last
    limit
    offset
    order_by
    preload
    search
    since
    start_date
    until
  )a

  @behaviour EctoShorts.QueryBuilder

  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}
  @type binding_alias :: atom()
  @type value :: any()
  @type opts :: keyword()
  @type filter ::
          :after
          | :before
          | :end_date
          | :first
          | :ids
          | :last
          | :limit
          | :offset
          | :order_by
          | :preload
          | :search
          | :since
          | :start_date
          | :until

  @impl EctoShorts.QueryBuilder
  @doc """
  Returns the list of supported filters for this query builder.

  These filters include pagination controls, ordering, ID matching,
  and timestamp-based range filters.

  ## Examples

      iex> EctoShorts.QueryBuilders.Common.filters()
      [:after, :before, :end_date, :first, :ids, :last, :limit, :offset, :order_by, :preload, :search, :since, :start_date, :until]
  """
  @spec filters() :: [filter()]
  def filters, do: @filters

  @impl EctoShorts.QueryBuilder
  @doc """
  Builds a query using the provided filter key and value.

  Each filter transforms the query according to a known expression:

    * `:ids` – Filters records where the primary key (`id`) matches any in the given list.
    * `:first` – Limits the number of results returned to the given value.
    * `:last` – Retrieves the last N records based on `inserted_at`, preserving original order.
    * `:limit` – Limits the number of results directly (same as `:first` but without side-effects).
    * `:offset` – Skips a number of results before returning the remainder.
    * `:order_by` – Sorts the results using the provided sort conditions.
    * `:preload` – Eager loads associations specified in the value.
    * `:after` – Filters records where the `id` is greater than the given value.
    * `:before` – Filters records where the `id` is less than the given value.
    * `:since` / `:start_date` – Filters records where `inserted_at` is greater than or equal to the value.
    * `:until` / `:end_date` – Filters records where `inserted_at` is less than or equal to the value.
    * `:search` – If the schema defines `by_search/2`, delegates to it. Otherwise, returns the original query.

  ## Examples

      iex> EctoShorts.QueryBuilders.Common.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :ids, [1, 2, 3])
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.id in ^[1, 2, 3]>

      iex> EctoShorts.QueryBuilders.Common.build_query(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :limit, 5)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, limit: ^5>
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
          queryable(),
          filter(),
          value()
        ) :: query() | queryable()
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
          queryable(),
          filter(),
          value(),
          opts()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value, opts \\ [])

  def build_query(query, current_binding, schema_module, :ids, values, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:id, {:==, values}}, opts)
  end

  def build_query(query, current_binding, _schema_module, :first, value, _opts) do
    CommonQuery.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :last, value, _opts) do
    query
    |> CommonQuery.exclude(:order_by)
    |> CommonQuery.order_by(current_binding, order_by: [desc: :inserted_at])
    |> CommonQuery.limit(current_binding, value)
    |> CommonQuery.subquery()
    |> CommonQuery.order_by(current_binding, :id)
  end

  def build_query(query, current_binding, _schema_module, :limit, value, _opts) do
    CommonQuery.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :offset, value, _opts) do
    CommonQuery.offset(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :order_by, value, _opts) do
    CommonQuery.order_by(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :preload, value, _opts) do
    CommonQuery.preload(query, current_binding, value)
  end

  def build_query(query, current_binding, schema_module, :after, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:id, {:>, value}}, opts)
  end

  def build_query(query, current_binding, schema_module, :before, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:id, {:<, value}}, opts)
  end

  def build_query(query, current_binding, schema_module, :since, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:inserted_at, {:>=, value}}, opts)
  end

  def build_query(query, current_binding, schema_module, :until, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:inserted_at, {:<=, value}}, opts)
  end

  def build_query(query, current_binding, schema_module, :start_date, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:inserted_at, {:>=, value}}, opts)
  end

  def build_query(query, current_binding, schema_module, :end_date, value, opts) do
    CommonQuery.where(query, current_binding, schema_module, {:inserted_at, {:<=, value}}, opts)
  end

  def build_query(query, _current_binding, schema_module, :search, value, _opts) do
    if function_exported?(schema_module, :by_search, 2) do
      schema_module.by_search(query, value)
    else
      query
    end
  end
end
