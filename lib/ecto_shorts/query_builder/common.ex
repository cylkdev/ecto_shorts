defmodule EctoShorts.QueryBuilder.Common do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.QueryBuilder.Common

  Provides common query-building functionality for
  EctoShorts.

  This module implements the `EctoShorts.QueryBuilder`
  behaviour for filters that are common to most Ecto
  schemas, such as pagination, ordering, and ID-based
  filtering.

  ## Examples

  ```elixir

      iex> EctoShorts.QueryBuilder.Common.filters()
      [
        :after, :before, :end_date, :first, :ids, :last,
        :limit, :offset, :order_by, :preload, :search,
        :since, :start_date, :until
      ]

      iex> EctoShorts.QueryBuilder.Common.build_query(
      ...>   MySchema, nil, nil, :limit, 10
      ...> )
      #Ecto.Query<...>

  ```
  """

  alias EctoShorts.CommonQueryExpressions

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

  @type binding :: atom() | nil

  @type filter_key ::
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

  @type filter_value :: term()

  @doc """
  Returns the list of supported filters for this query builder.

  ## Examples

      iex> EctoShorts.QueryBuilder.Common.filters()
      [:after, :before, :end_date, :first, :ids, :last, :limit, :offset, :order_by, :preload, :search, :since, :start_date, :until]
  """
  @spec filters() :: [filter_key()]
  def filters, do: @filters

  @impl EctoShorts.QueryBuilder
  @doc """
  Builds a query based on the given filter key and value.

  Supported filters include:

    * `:ids` - Filters by a list of IDs.

    * `:first` - Limits the result to the first N entries.

    * `:last` - Returns the last N entries, ordered by `inserted_at`.

    * `:limit` - Limits the number of results.

    * `:offset` - Offsets the results by a given number.

    * `:order_by` - Orders the results.

    * `:preload` - Preloads associations.

    * `:after` - Filters for IDs greater than the given value.

    * `:before` - Filters for IDs less than the given value.

    * `:since` / `:start_date` - Filters for `inserted_at` >= value.

    * `:until` / `:end_date` - Filters for `inserted_at` <= value.

    * `:search` - If the schema module defines a `by_search/2` function, it
      will be called to apply custom search logic. Otherwise, the query is
      returned unchanged.

  ## Examples

      iex> EctoShorts.QueryBuilder.Common.build_query(query, nil, MySchema, :ids, [1, 2, 3])
      #Ecto.Query<...>

      iex> EctoShorts.QueryBuilder.Common.build_query(query, nil, MySchema, :limit, 5)
      #Ecto.Query<...>
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding() | nil,
          queryable(),
          filter_key(),
          filter_value()
        ) :: query() | queryable()
  def build_query(query, current_binding, _schema_module, :ids, values) do
    CommonQueryExpressions.where(query, current_binding, :id, :==, values)
  end

  def build_query(query, current_binding, _schema_module, :first, value) do
    CommonQueryExpressions.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :last, value) do
    query
    |> CommonQueryExpressions.exclude(:order_by)
    |> CommonQueryExpressions.order_by(current_binding, order_by: [desc: :inserted_at])
    |> CommonQueryExpressions.limit(current_binding, value)
    |> CommonQueryExpressions.subquery()
    |> CommonQueryExpressions.order_by(current_binding, :id)
  end

  def build_query(query, current_binding, _schema_module, :limit, value) do
    CommonQueryExpressions.limit(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :offset, value) do
    CommonQueryExpressions.offset(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :order_by, value) do
    CommonQueryExpressions.order_by(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :preload, value) do
    CommonQueryExpressions.preload(query, current_binding, value)
  end

  def build_query(query, current_binding, _schema_module, :after, value) do
    CommonQueryExpressions.where(query, current_binding, :id, :>, value)
  end

  def build_query(query, current_binding, _schema_module, :before, value) do
    CommonQueryExpressions.where(query, current_binding, :id, :<, value)
  end

  def build_query(query, current_binding, _schema_module, :since, value) do
    CommonQueryExpressions.where(query, current_binding, :inserted_at, :>=, value)
  end

  def build_query(query, current_binding, _schema_module, :until, value) do
    CommonQueryExpressions.where(query, current_binding, :inserted_at, :<=, value)
  end

  def build_query(query, current_binding, _schema_module, :start_date, value) do
    CommonQueryExpressions.where(query, current_binding, :inserted_at, :>=, value)
  end

  def build_query(query, current_binding, _schema_module, :end_date, value) do
    CommonQueryExpressions.where(query, current_binding, :inserted_at, :<=, value)
  end

  def build_query(query, schema_module, :search, value) do
    if function_exported?(schema_module, :by_search, 2) do
      schema_module.by_search(query, value)
    else
      query
    end
  end
end
