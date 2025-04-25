defmodule EctoShorts.CommonQueryExpressions.API do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.CommonQueryExpressions.API

  Provides wrapper functions to simplify the usage of the
  `Ecto.Query` API. These helpers abstract common patterns
  for building dynamic queries, handling bindings, and
  composing query fragments in a way that is reusable and
  consistent.
  """

  alias Ecto.Query

  require Ecto.Query

  @type source :: binary()

  @type dynamic_expr :: Ecto.Query.dynamic_expr()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type binding() :: atom() | nil

  @type key :: atom()

  @type value :: any()

  @type join_type :: :association | :subquery

  @type field :: atom()

  @type prefix :: binary()

  @type opts :: keyword()

  @default_join_opts [qualifier: :inner, on: true]

  @doc """
  Merges two dynamic query expressions using `and`.
  If the first argument is `nil`, returns the second.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.merge_dynamic(nil, dynamic([q], q.id > 1))
      #Ecto.Query.DynamicExpr<...>

      iex> EctoShorts.CommonQueryExpressions.API.merge_dynamic(dynamic([q], q.id > 1), dynamic([q], q.name == "foo"))
      #Ecto.Query.DynamicExpr<...>
  """
  @spec merge_dynamic(nil | dynamic_expr(), dynamic_expr()) :: dynamic_expr()
  def merge_dynamic(nil, dyn), do: dyn
  def merge_dynamic(dyn_a, dyn_b), do: Query.dynamic(^dyn_a and ^dyn_b)

  @doc """
  Builds a dynamic query expression.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.dynamic(:user, user.age > 18)
      #Ecto.Query.DynamicExpr<...>

      iex> EctoShorts.CommonQueryExpressions.API.dynamic(nil, q.id == 1)
      #Ecto.Query.DynamicExpr<...>
  """
  @spec dynamic(binding() | nil, value()) :: dynamic_expr()
  def dynamic(current_binding \\ nil, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], ^value)
    else
      Query.dynamic([q], ^value)
    end
  end

  @doc """
  Sets the `from` clause on a query.

  ## Options

    * `:as` - Sets a binding for the source in the query, allowing you to
      reference it by name in other query expressions.

    * `:prefix` - Sets the database schema (prefix) for the query source.

    * `:query_prefix` - Sets the prefix for the query itself, which can be
      used to override the prefix for subqueries or fragments.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.from(MySchema, as: :user)
      #Ecto.Query<...>

      iex> EctoShorts.CommonQueryExpressions.API.from(MySchema, prefix: "custom_schema")
      #Ecto.Query<...>
  """
  @spec from(query() | queryable() | source_queryable()) :: query()
  @spec from(query() | queryable() | source_queryable(), opts()) :: query()
  def from(query, opts \\ []) do
    as = opts[:as]

    prefix = opts[:prefix]

    if as do
      query
      |> Query.from(as: ^as, prefix: ^prefix)
      |> maybe_put_query_prefix(opts)
    else
      query
      |> Query.from(prefix: ^prefix)
      |> maybe_put_query_prefix(opts)
    end
  end

  defp maybe_put_query_prefix(query, opts) do
    case opts[:query_prefix] do
      nil -> query
      prefix -> put_query_prefix(query, prefix)
    end
  end

  @doc """
  Sets the query prefix (database schema) for the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.put_query_prefix(query, "custom_schema")
      #Ecto.Query<...>
  """
  @spec put_query_prefix(
          query() | queryable() | source_queryable(),
          prefix()
        ) :: query()
  def put_query_prefix(query, prefix) do
    Query.put_query_prefix(query, prefix)
  end

  @doc """
  Wraps a query as a subquery.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.subquery(query)
      #Ecto.Query<...>
  """
  @spec subquery(query() | queryable() | source_queryable()) :: query()
  @spec subquery(query() | queryable() | source_queryable(), opts()) ::
          query()
  def subquery(query, opts \\ []) do
    Query.subquery(query, opts)
  end

  @doc """
  Excludes a field (such as `:order_by`) from the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.exclude(query, :order_by)
      #Ecto.Query<...>
  """
  @spec exclude(query() | queryable() | source_queryable(), field :: field()) :: query()
  def exclude(query, field) do
    Query.exclude(query, field)
  end

  @doc """
  Limits the number of results returned by the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.limit(query, nil, 10)
      #Ecto.Query<...>

      iex> EctoShorts.CommonQueryExpressions.API.limit(query, :user, 5)
      #Ecto.Query<...>
  """
  @spec limit(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def limit(query, current_binding, value) do
    if current_binding do
      Query.limit(query, [{^current_binding, q}], ^value)
    else
      Query.limit(query, [q], ^value)
    end
  end

  @doc """
  Offsets the results returned by the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.offset(query, nil, 20)
      #Ecto.Query<...>
  """
  @spec offset(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def offset(query, current_binding, value) do
    if current_binding do
      Query.offset(query, [{^current_binding, q}], ^value)
    else
      Query.offset(query, [q], ^value)
    end
  end

  @doc """
  Groups the results by the given value.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.group_by(query, nil, :category)
      #Ecto.Query<...>
  """
  @spec group_by(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def group_by(query, current_binding, value) do
    if current_binding do
      Query.group_by(query, [{^current_binding, q}], ^value)
    else
      Query.group_by(query, [q], ^value)
    end
  end

  @doc """
  Orders the results by the given value.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.order_by(query, nil, [asc: :inserted_at])
      #Ecto.Query<...>
  """
  @spec order_by(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def order_by(query, current_binding, value) do
    if current_binding do
      Query.order_by(query, [{^current_binding, q}], ^value)
    else
      Query.order_by(query, [q], ^value)
    end
  end

  @doc """
  Preloads associations on the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.preload(query, nil, :comments)
      #Ecto.Query<...>
  """
  @spec preload(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def preload(query, current_binding, value) do
    if current_binding do
      Query.preload(query, [{^current_binding, q}], ^value)
    else
      Query.preload(query, [q], ^value)
    end
  end

  @doc """
  Selects fields or expressions from the query, supporting `true`,
  `{:map, values}`, `{:struct, values}`, or a custom value.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.select(query, nil, true)
      #Ecto.Query<...>

      iex> EctoShorts.CommonQueryExpressions.API.select(query, nil, {:map, [:id, :name]})
      #Ecto.Query<...>
  """
  @spec select(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def select(query, current_binding, true) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], q)
    else
      Query.select(query, [q], q)
    end
  end

  def select(query, current_binding, {:map, values}) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], map(q, ^values))
    else
      Query.select(query, [q], map(q, ^values))
    end
  end

  def select(query, current_binding, {:struct, values}) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], struct(q, ^values))
    else
      Query.select(query, [q], struct(q, ^values))
    end
  end

  def select(query, current_binding, value) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], ^value)
    else
      Query.select(query, [q], ^value)
    end
  end

  @doc """
  Merges additional fields into the select clause, supporting `true` or a custom value.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.select_merge(query, nil, true)
      #Ecto.Query<...>
  """
  @spec select_merge(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def select_merge(query, current_binding, true) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], q)
    else
      Query.select_merge(query, [q], q)
    end
  end

  def select_merge(query, current_binding, value) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], ^value)
    else
      Query.select_merge(query, [q], ^value)
    end
  end

  @doc """
  Adds a join to the query for an association or subquery.

  ## Options

    * `:qualifier` - Specifies the join type, such as `:inner`, `:left`, or `:right`.
      Defaults to `:inner`.

    * `:on` - Specifies the join condition. Can be a boolean or a dynamic expression.
      Defaults to `true` (a cross join).

    * `:prefix` - Sets the database schema (prefix) for the join source.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.join(query, :user, :association, :posts, qualifier: :left)
      #Ecto.Query<...>
  """
  @spec join(
          query() | queryable() | source_queryable(),
          binding() | {binding(), binding()} | nil,
          join_type(),
          key(),
          opts()
        ) :: query()
  def join(query, {current_binding, join_binding_alias}, :association, key, opts) do
    opts = Keyword.merge(@default_join_opts, opts)

    qual = opts[:qualifier]

    on = opts[:on]

    prefix = opts[:prefix]

    if current_binding do
      Query.with_named_binding(query, join_binding_alias, fn query, as ->
        Query.join(query, qual, [{^current_binding, q}], assoc(q, ^key),
          as: ^as,
          on: ^on,
          prefix: ^prefix
        )
      end)
    else
      Query.with_named_binding(query, join_binding_alias, fn query, as ->
        Query.join(query, qual, [q], assoc(q, ^key),
          as: ^as,
          on: ^on,
          prefix: ^prefix
        )
      end)
    end
  end

  def join(query, current_binding, :association, key, opts) do
    opts = Keyword.merge(@default_join_opts, opts)

    qual = opts[:qualifier]

    on = opts[:on]

    prefix = opts[:prefix]

    if current_binding do
      Query.join(query, qual, [{^current_binding, q}], assoc(q, ^key), on: ^on, prefix: ^prefix)
    else
      Query.join(query, qual, [q], assoc(q, ^key), on: ^on, prefix: ^prefix)
    end
  end

  def join(query, {current_binding, join_binding_alias}, :subquery, from, opts) do
    opts = Keyword.merge(@default_join_opts, opts)

    qual = opts[:qualifier]

    on = opts[:on]

    prefix = opts[:prefix]

    Query.with_named_binding(query, join_binding_alias, fn query, as ->
      if current_binding do
        Query.join(query, qual, [{^current_binding, q}], subquery(from),
          as: ^as,
          on: ^on,
          prefix: ^prefix
        )
      else
        Query.join(query, qual, [q], subquery(from), as: ^as, on: ^on, prefix: ^prefix)
      end
    end)
  end

  def join(query, current_binding, :subquery, from, opts) do
    opts = Keyword.merge(@default_join_opts, opts)

    qual = opts[:qualifier]

    on = opts[:on]

    prefix = opts[:prefix]

    if current_binding do
      Query.join(query, qual, [{^current_binding, q}], subquery(from), on: ^on, prefix: ^prefix)
    else
      Query.join(query, qual, [q], subquery(from), on: ^on, prefix: ^prefix)
    end
  end

  @doc """
  Adds an `or_where` condition to the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.or_where(query, nil, dynamic([q], q.id > 1))
      #Ecto.Query<...>
  """
  @spec or_where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def or_where(query, current_binding, value) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], ^value)
    else
      Query.or_where(query, [q], ^value)
    end
  end

  @doc """
  Adds a `where` condition to the query.

  ## Examples

      iex> EctoShorts.CommonQueryExpressions.API.where(query, nil, dynamic([q], q.id > 1))
      #Ecto.Query<...>
  """
  @spec where(
          query() | queryable() | source_queryable(),
          binding() | nil,
          value()
        ) :: query()
  def where(query, current_binding, value) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], ^value)
    else
      Query.where(query, [q], ^value)
    end
  end
end
