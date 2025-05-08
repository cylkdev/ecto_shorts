defmodule EctoShorts.CommonQuery do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Build Ecto queries dynamically without needing to import `Ecto.Query`.

  `EctoShorts.CommonQuery` makes it easier to compose `Ecto`
  queries by providing a clean, data-driven interface for building
  query expressions.

  Instead of importing the `Ecto.Query` DSL and manually writing macros
  like `from`, `where`, or `select`, you can use plain Elixir data (maps
  and keyword lists) to express filters, joins, ordering, and projections.

  ## Getting Started

  Here’s a simple example:

      alias EctoShorts.CommonQuery

      User
      |> CommonQuery.from(as: :user)
      |> CommonQuery.where(:user, %{active: true})
      |> CommonQuery.order_by(:user, [asc: :inserted_at])
      |> CommonQuery.limit(nil, 10)

  You don’t need to `import Ecto.Query`, and there are no macros to learn,
  just use functions that work with data.

  This API is split into the following components:

    * `EctoShorts.CommonQuery.API` - Core helpers for building Ecto queries
      using bindings, `select`, `join`, `limit`, and other common clauses. Acts as a
      lightweight wrapper around `Ecto.Query` with runtime-friendly syntax.

    * `EctoShorts.QueryExpression.Postgres` - Provides `where` and `or_where`
      logic that supports map-based filters, operator parsing, and Postgres-aware
      behavior like array handling.

    * `EctoShorts.QueryExpression.Postgres.Array` - Enables comparisons and
      matches on array fields in Postgres using `ANY`, `ILIKE`, `LIKE`, and regex.

    * `EctoShorts.QueryExpression.Postgres.Field` - Handles scalar field filters,
      including support for case-insensitive comparison, null checking, and multiple values.
  """

  alias EctoShorts.{
    DynamicExpression,
    ExpressionBuilder
  }

  alias Ecto.Query

  require Ecto.Query

  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}
  @type dynamic_expr :: Ecto.Query.DynamicExpr.t()
  @type join :: :association | :subquery
  @type limit :: integer()
  @type offset :: integer()
  @type binding_alias :: atom()
  @type prefix :: binary()
  @type field :: atom()
  @type operator :: atom()
  @type key :: atom()
  @type value :: any()
  @type params :: map()
  @type opts :: keyword()

  @doc """
  Merges two dynamic expressions with `and`.

  Returns the second expression if the first is `nil`.

  ## Examples

      iex> EctoShorts.CommonQuery.merge_dynamic(nil, dynamic([q], q.id > 1))
      #Ecto.Query.DynamicExpr<...>

      iex> dyn1 = dynamic([q], q.id > 1)
      ...> dyn2 = dynamic([q], q.active == true)
      ...> EctoShorts.CommonQuery.merge_dynamic(dyn1, dyn2)
      #Ecto.Query.DynamicExpr<...>
  """
  @spec merge_dynamic(nil | dynamic_expr(), :and | :or, dynamic_expr()) :: dynamic_expr()
  def merge_dynamic(dyn_a, operator \\ :and, dyn_b)
  def merge_dynamic(nil, _operator, dyn), do: dyn
  def merge_dynamic(dyn_a, :or, dyn_b), do: Query.dynamic(^dyn_a or ^dyn_b)
  def merge_dynamic(dyn_a, :and, dyn_b), do: Query.dynamic(^dyn_a and ^dyn_b)

  @doc """
  Builds a binding-aware dynamic expression.

  If a binding is provided, the dynamic will be created using that named binding.
  If no binding is given, it defaults to `q`.

  ## Examples

      iex> EctoShorts.CommonQuery.dynamic(:user, dynamic([user], user.age > 18))
      #Ecto.Query.DynamicExpr<...>

      iex> EctoShorts.CommonQuery.dynamic(nil, dynamic([q], q.id == 1))
      #Ecto.Query.DynamicExpr<...>
  """
  @spec dynamic(binding_alias() | nil, value()) :: dynamic_expr()
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

    * `:as` — Sets a binding for the source
    * `:prefix` — Schema prefix for the table
    * `:query_prefix` — Prefix for subqueries or fragments

  ## Example

      iex> EctoShorts.CommonQuery.from(User, as: :user)
  """
  @spec from(query() | queryable() | source_queryable()) :: query()
  @spec from(query() | queryable() | source_queryable(), params()) :: query()
  def from(query, params \\ %{}) do
    as = params[:as]

    prefix = params[:prefix]

    if as do
      query
      |> Query.from(as: ^as, prefix: ^prefix)
      |> maybe_put_query_prefix(params)
    else
      query
      |> Query.from(prefix: ^prefix)
      |> maybe_put_query_prefix(params)
    end
  end

  defp maybe_put_query_prefix(query, params) do
    case params[:query_prefix] do
      nil -> query
      prefix -> put_query_prefix(query, prefix)
    end
  end

  @doc """
  Overrides the query’s schema prefix (useful for multi-tenancy).

  ## Example

      iex> EctoShorts.CommonQuery.put_query_prefix(query, "tenant_123")
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

      iex> EctoShorts.CommonQuery.subquery(query)
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

      iex> EctoShorts.CommonQuery.exclude(query, :order_by)
      #Ecto.Query<...>
  """
  @spec exclude(query() | queryable() | source_queryable(), field :: field()) :: query()
  def exclude(query, field) do
    Query.exclude(query, field)
  end

  @doc """
  Limits the number of results returned by the query.

  ## Examples

      iex> EctoShorts.CommonQuery.limit(query, nil, 10)
      #Ecto.Query<...>

      iex> EctoShorts.CommonQuery.limit(query, :user, 5)
      #Ecto.Query<...>
  """
  @spec limit(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
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

      iex> EctoShorts.CommonQuery.offset(query, nil, 20)
      #Ecto.Query<...>
  """
  @spec offset(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
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

      iex> EctoShorts.CommonQuery.group_by(query, nil, :category)
      #Ecto.Query<...>
  """
  @spec group_by(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
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

      iex> EctoShorts.CommonQuery.order_by(query, nil, [asc: :inserted_at])
      #Ecto.Query<...>
  """
  @spec order_by(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
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

      iex> EctoShorts.CommonQuery.preload(query, nil, :comments)
      #Ecto.Query<...>
  """
  @spec preload(
          query() | queryable() | source_queryable(),
          binding_alias() | nil,
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
  Dynamically selects fields or associations.

  Accepts values such as `true`, `{:map, fields}`, `{:struct, fields}`, or
  nested maps for associations.

  Applies selection recursively via `QueryHelpers`.

  See: `EctoShorts.CommonQuery.select/3`
  """
  @spec select(query() | queryable() | source_queryable(), binding_alias() | nil, value()) ::
          query()
  def select(query, current_binding, values) when is_list(values) do
    if Keyword.keyword?(values) do
      ExpressionBuilder.apply_expressions(query, values, fn value, query ->
        select(query, current_binding, value)
      end)
    else
      select(query, current_binding, {:struct, values})
    end
  end

  def select(query, current_binding, params) when is_map(params) do
    ExpressionBuilder.apply_expressions(query, params, fn {key, value}, query ->
      select(query, current_binding, {key, value})
    end)
  end

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
  Dynamically merges fields into an existing select.

  Behaves like `select/3` but uses `select_merge/3` internally.

  See: `EctoShorts.CommonQuery.select_merge/3`
  """
  @spec select_merge(query() | queryable() | source_queryable(), binding_alias() | nil, value()) ::
          query()
  def select_merge(query, current_binding, values) when is_list(values) do
    if Keyword.keyword?(values) do
      ExpressionBuilder.apply_expressions(query, values, fn value, query ->
        select_merge(query, current_binding, value)
      end)
    else
      if current_binding do
        Query.select_merge(query, [{^current_binding, q}], ^values)
      else
        Query.select_merge(query, [q], ^values)
      end
    end
  end

  def select_merge(query, current_binding, params) when is_map(params) do
    ExpressionBuilder.apply_expressions(query, params, fn {key, value}, query ->
      select_merge(query, current_binding, {key, value})
    end)
  end

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
  Adds a join to the query for an association or subquery, supporting both
  bound and unbound contexts.

  This helper wraps `Ecto.Query.join/5`, with support for association joins
  via `assoc/2` and subquery joins via `subquery/1`. It also supports
  named bindings, which are useful when building layered queries that require
  binding reuse.

  ## Examples

      iex> EctoShorts.CommonQuery.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments)>

      iex> EctoShorts.CommonQuery.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{as: :comments})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :comments>

      iex> EctoShorts.CommonQuery.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{on: true})
      Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments)>

      iex> EctoShorts.CommonQuery.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{as: :comments, on: %{id: 2}})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), on: c1.id == ^2>

      iex> EctoShorts.CommonQuery.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{as: :comments, on: %{id: %{>=: 2}}})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), on: c1.id >= ^2>
  """
  def join(query, current_binding, schema_module, join_type, arg, params \\ %{}, opts \\ [])

  def join(query, current_binding, schema_module, :association, key, params, opts) do
    qual = params[:qualifier] || :inner

    prefix = params[:prefix]

    as = params[:as]

    on = join_on(as, schema_module, params, opts)

    join_assoc(query, current_binding, key, {as, qual, on, prefix})
  end

  def join(query, current_binding, schema_module, :subquery, from_query, params, opts) do
    qual = params[:qualifier] || :inner

    prefix = params[:prefix]

    as = params[:as]

    subquery_opts =
      case params[:options] do
        nil -> []
        opts when is_map(opts) -> Map.to_list(opts)
        opts -> opts
      end

    on = join_on(as, schema_module, params, opts)

    join_subquery(query, current_binding, from_query, subquery_opts, {as, qual, on, prefix})
  end

  defp join_assoc(query, current_binding, key, {as, qual, on, prefix}) do
    if is_nil(as) or as === false do
      if current_binding do
        Query.join(
          query,
          qual,
          [{^current_binding, q}],
          assoc(q, ^key),
          on: ^on,
          prefix: ^prefix
        )
      else
        Query.join(
          query,
          qual,
          [q],
          assoc(q, ^key),
          on: ^on,
          prefix: ^prefix
        )
      end
    else
      Query.with_named_binding(query, as, fn query, as ->
        if current_binding do
          Query.join(
            query,
            qual,
            [{^current_binding, q}],
            assoc(q, ^key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        else
          Query.join(
            query,
            qual,
            [q],
            assoc(q, ^key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end)
    end
  end

  defp join_subquery(query, current_binding, from, subquery_opts, {as, qual, on, prefix}) do
    if is_nil(as) or as === false do
      if current_binding do
        Query.join(
          query,
          qual,
          [{^current_binding, q}],
          subquery(from, subquery_opts),
          on: ^on,
          prefix: ^prefix
        )
      else
        Query.join(
          query,
          qual,
          [q],
          subquery(from, subquery_opts),
          on: ^on,
          prefix: ^prefix
        )
      end
    else
      Query.with_named_binding(query, as, fn query, as ->
        if current_binding do
          Query.join(
            query,
            qual,
            [{^current_binding, q}],
            subquery(from, subquery_opts),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        else
          Query.join(
            query,
            qual,
            [q],
            subquery(from, subquery_opts),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end)
    end
  end

  defp join_on(as, schema_module, params, opts) do
    case params[:on] do
      nil ->
        true

      true ->
        true

      on_params ->
        {schema_module, on_params} = Map.pop(on_params, :queryable, schema_module)

        build_dynamic_expression(
          as,
          schema_module,
          on_params,
          opts
        )
    end
  end

  def or_where(query, current_binding, schema_module, value, opts \\ [])

  def or_where(query, current_binding, schema_module, {key, value}, opts) do
    expr = build_dynamic_expression(current_binding, schema_module, %{key => value}, opts)

    or_where(query, schema_module, nil, expr, opts)
  end

  def or_where(query, current_binding, schema_module, values, opts) when is_list(values) do
    if Keyword.keyword?(values) do
      or_where(query, current_binding, schema_module, Map.new(values), opts)
    else
      Enum.reduce(values, query, fn value, query ->
        or_where(query, current_binding, schema_module, value, opts)
      end)
    end
  end

  def or_where(query, current_binding, schema_module, params, opts)
      when is_map(params) and not is_struct(params) do
    expr = build_dynamic_expression(current_binding, schema_module, params, opts)

    or_where(query, schema_module, nil, expr, opts)
  end

  def or_where(query, current_binding, _schema_module, value, _opts) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], ^value)
    else
      Query.or_where(query, [q], ^value)
    end
  end

  def where(query, current_binding, schema_module, value, opts \\ [])

  def where(query, current_binding, schema_module, {key, value}, opts) do
    expr = build_dynamic_expression(current_binding, schema_module, %{key => value}, opts)

    where(query, schema_module, nil, expr, opts)
  end

  def where(query, current_binding, schema_module, values, opts) when is_list(values) do
    if Keyword.keyword?(values) do
      where(query, current_binding, schema_module, Map.new(values), opts)
    else
      Enum.reduce(values, query, fn value, query ->
        where(query, current_binding, schema_module, value, opts)
      end)
    end
  end

  def where(query, current_binding, schema_module, params, opts)
      when is_map(params) and not is_struct(params) do
    expr = build_dynamic_expression(current_binding, schema_module, params, opts)

    where(query, schema_module, nil, expr, opts)
  end

  def where(query, current_binding, _schema_module, value, _opts) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], ^value)
    else
      Query.where(query, [q], ^value)
    end
  end

  defp build_dynamic_expression(current_binding, schema_module, params, opts) do
    {dynamic_opts, params} = Map.pop(params, :dynamic, %{})

    condition = dynamic_opts[:condition] || :and

    dynamic_opts
    |> Map.get(:source)
    |> ExpressionBuilder.apply_expressions(
      params,
      fn {key, value}, dyn ->
        DynamicExpression.build_dynamic_expression(
          dyn,
          current_binding,
          condition,
          schema_module,
          key,
          value,
          opts
        )
      end
    )
  end
end
