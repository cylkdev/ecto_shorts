defmodule EctoShorts.CommonQueries do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Build Ecto queries dynamically without needing to import `Ecto.Query`.

  `EctoShorts.CommonQueries` makes it easier to compose `Ecto`
  queries by providing a clean, data-driven interface for building
  query expressions.

  Instead of importing the `Ecto.Query` DSL and manually writing macros
  like `from`, `where`, or `select`, you can use plain Elixir data (maps
  and keyword lists) to express filters, joins, ordering, and projections.

  ## Getting Started

  Here’s a simple example:

      alias EctoShorts.CommonQueries

      User
      |> CommonQueries.from(as: :user)
      |> CommonQueries.where(:user, %{active: true})
      |> CommonQueries.order_by(:user, [asc: :inserted_at])
      |> CommonQueries.limit(nil, 10)

  You don’t need to `import Ecto.Query`, and there are no macros to learn,
  just use functions that work with data.

  This API is split into the following components:

    * `EctoShorts.CommonQueries.API` - Core helpers for building Ecto queries
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

  alias EctoShorts.CommonQuery
  alias Ecto.Query

  alias EctoShorts.{
    DynamicBuilder,
    QueryHelpers
  }

  require Ecto.Query

  @type subquery :: Ecto.SubQuery.t()
  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type schema_data :: Ecto.Schema.t()
  @type schema_metadata :: Ecto.Schema.Metadata.t()
  @type sourceable :: schema_module() | {schema_source(), schema_module()}
  @type prefix :: binary() | nil
  @type changeset :: Ecto.Changeset.t()
  @type dynamic_expr :: %Ecto.Query.DynamicExpr{}
  @type maybe_dynamic_expr :: dynamic_expr() | nil
  @type binding_alias :: atom()

  @type condition :: :and | :or
  @type join_type :: :association | :subquery
  @type limit :: non_neg_integer()
  @type offset :: integer()

  @type key :: atom()
  @type value :: any()
  @type operator :: atom()
  @type params :: map() | keyword()
  @type opts :: keyword()

  @doc """
  Merges two dynamic expressions with `and`.

  Returns the second expression if the first is `nil`.

  ## Examples

      iex> EctoShorts.CommonQueries.merge_dynamic(nil, dynamic([q], q.id > 1))
      #Ecto.Query.DynamicExpr<...>

      iex> dyn1 = dynamic([q], q.id > 1)
      ...> dyn2 = dynamic([q], q.active == true)
      ...> EctoShorts.CommonQueries.merge_dynamic(dyn1, dyn2)
      #Ecto.Query.DynamicExpr<...>
  """
  @spec merge_dynamic(maybe_dynamic_expr(), condition(), dynamic_expr()) :: dynamic_expr()
  def merge_dynamic(nil, _operator, dyn), do: dyn
  def merge_dynamic(dyn_a, :or, dyn_b), do: Query.dynamic(^dyn_a or ^dyn_b)
  def merge_dynamic(dyn_a, :and, dyn_b), do: Query.dynamic(^dyn_a and ^dyn_b)

  @doc """
  Builds a binding-aware dynamic expression.

  If a binding is provided, the dynamic will be created using that named binding.
  If no binding is given, it defaults to `q`.

  ## Examples

      iex> EctoShorts.CommonQueries.dynamic(:user, dynamic([user], user.age > 18))
      #Ecto.Query.DynamicExpr<...>

      iex> EctoShorts.CommonQueries.dynamic(nil, dynamic([q], q.id == 1))
      #Ecto.Query.DynamicExpr<...>
  """
  @spec dynamic(binding_alias(), value()) :: dynamic_expr()
  def dynamic(binding_alias, value) do
    if binding_alias do
      Query.dynamic([{^binding_alias, q}], ^value)
    else
      Query.dynamic([q], ^value)
    end
  end

  @spec dynamic(
          schema_module(),
          binding_alias(),
          params(),
          opts()
        ) :: dynamic_expr()
  def dynamic(schema_module, binding_alias, params, opts) when is_map(params) do
    dynamic(schema_module, binding_alias, Map.to_list(params), opts)
  end

  def dynamic(schema_module, binding_alias, params, opts) do
    with nil <- query_dynamic(schema_module, binding_alias, params, opts) do
      dynamic(binding_alias, true)
    end
  end

  defp query_dynamic(schema_module, binding_alias, params, opts) do
    binding_alias = params[:as] || binding_alias

    condition = params[:condition] || :and

    params
    |> Keyword.get(:source)
    |> QueryHelpers.apply_expression(
      params,
      fn {key, value}, dyn ->
        DynamicBuilder.build_dynamic(
          schema_module,
          dyn,
          binding_alias,
          condition,
          key,
          value,
          opts
        )
      end
    )
  end

  @doc """
  Sets the `from` clause on a query.

  ## Options

    * `:as` — Sets a binding for the source
    * `:prefix` — Schema prefix for the table
    * `:query_prefix` — Prefix for subqueries or fragments

  ## Example

      iex> EctoShorts.CommonQueries.from(User, as: :user)
  """
  @spec from(query() | sourceable(), params()) :: query()
  def from(query, params) do
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

      iex> EctoShorts.CommonQueries.put_query_prefix(query, "tenant_123")
  """
  @spec put_query_prefix(
          query() | sourceable(),
          prefix()
        ) :: query()
  def put_query_prefix(query, prefix) do
    Query.put_query_prefix(query, prefix)
  end

  @doc """
  Wraps a query as a subquery.

  ## Examples

      iex> EctoShorts.CommonQueries.subquery(query)
      #Ecto.Query<...>
  """
  @spec subquery(query() | sourceable() | subquery(), opts()) :: subquery()
  def subquery(query, opts) do
    Query.subquery(query, opts)
  end

  @doc """
  Excludes a field (such as `:order_by`) from the query.

  ## Examples

      iex> EctoShorts.CommonQueries.exclude(query, :order_by)
      #Ecto.Query<...>
  """
  @spec exclude(query() | sourceable(), key()) :: query()
  def exclude(query, key) do
    Query.exclude(query, key)
  end

  @doc """
  Limits the number of results returned by the query.

  ## Examples

      iex> EctoShorts.CommonQueries.limit(query, nil, 10)
      #Ecto.Query<...>

      iex> EctoShorts.CommonQueries.limit(query, :user, 5)
      #Ecto.Query<...>
  """
  @spec limit(
          query() | sourceable(),
          binding_alias(),
          value()
        ) :: query()
  def limit(query, binding_alias, value) do
    if binding_alias do
      Query.limit(query, [{^binding_alias, q}], ^value)
    else
      Query.limit(query, [q], ^value)
    end
  end

  @doc """
  Offsets the results returned by the query.

  ## Examples

      iex> EctoShorts.CommonQueries.offset(query, nil, 20)
      #Ecto.Query<...>
  """
  @spec offset(
          query() | sourceable(),
          binding_alias(),
          value()
        ) :: query()
  def offset(query, binding_alias, value) do
    if binding_alias do
      Query.offset(query, [{^binding_alias, q}], ^value)
    else
      Query.offset(query, [q], ^value)
    end
  end

  @doc """
  Groups the results by the given value.

  ## Examples

      iex> EctoShorts.CommonQueries.group_by(query, nil, :category)
      #Ecto.Query<...>
  """
  @spec group_by(
          query() | sourceable(),
          binding_alias(),
          value()
        ) :: query()
  def group_by(query, binding_alias, value) do
    if binding_alias do
      Query.group_by(query, [{^binding_alias, q}], ^value)
    else
      Query.group_by(query, [q], ^value)
    end
  end

  @doc """
  Orders the results by the given value.

  ## Examples

      iex> EctoShorts.CommonQueries.order_by(query, nil, [asc: :inserted_at])
      #Ecto.Query<...>
  """
  @spec order_by(
          query() | sourceable(),
          binding_alias(),
          value()
        ) :: query()
  def order_by(query, binding_alias, value) do
    if binding_alias do
      Query.order_by(query, [{^binding_alias, q}], ^value)
    else
      Query.order_by(query, [q], ^value)
    end
  end

  @doc """
  Preloads associations on the query.

  ## Examples

      iex> EctoShorts.CommonQueries.preload(query, nil, :comments)
      #Ecto.Query<...>
  """
  @spec preload(
          query() | sourceable(),
          binding_alias(),
          value()
        ) :: query()
  def preload(query, binding_alias, value) do
    if binding_alias do
      Query.preload(query, [{^binding_alias, q}], ^value)
    else
      Query.preload(query, [q], ^value)
    end
  end

  @doc """
  Dynamically selects fields or associations.

  Accepts values such as `true`, `{:map, fields}`, `{:struct, fields}`, or
  nested maps for associations.

  Applies selection recursively via `CommonQuery`.

  See: `EctoShorts.CommonQueries.select/3`
  """
  @spec select(query() | sourceable(), binding_alias(), params() | value()) :: query()
  def select(query, binding_alias, params) when is_map(params) do
    select(query, binding_alias, Map.to_list(params))
  end

  def select(query, binding_alias, params) when is_list(params) do
    if Keyword.keyword?(params) do
      if Keyword.has_key?(params, :expression) do
        query_select(query, binding_alias, params[:expression])
      else
        QueryHelpers.apply_expression(query, params, fn {key, value}, query ->
          query_select(query, binding_alias, {key, value})
        end)
      end
    else
      query_select(query, binding_alias, params)
    end
  end

  def select(query, binding_alias, value) do
    query_select(query, binding_alias, value)
  end

  defp query_select(query, binding_alias, true) do
    if binding_alias do
      Query.select(query, [{^binding_alias, q}], q)
    else
      Query.select(query, [q], q)
    end
  end

  defp query_select(query, binding_alias, {:map, values}) do
    if binding_alias do
      Query.select(query, [{^binding_alias, q}], map(q, ^values))
    else
      Query.select(query, [q], map(q, ^values))
    end
  end

  defp query_select(query, binding_alias, {:struct, values}) do
    if binding_alias do
      Query.select(query, [{^binding_alias, q}], struct(q, ^values))
    else
      Query.select(query, [q], struct(q, ^values))
    end
  end

  defp query_select(query, binding_alias, value) do
    if binding_alias do
      Query.select(query, [{^binding_alias, q}], ^value)
    else
      Query.select(query, [q], ^value)
    end
  end

  @doc """
  Dynamically merges fields into an existing select.

  Behaves like `select/3` but uses `select_merge/3` internally.

  See: `EctoShorts.CommonQueries.select_merge/3`
  """
  @spec select_merge(query() | sourceable(), binding_alias(), params() | value()) ::
          query()
  def select_merge(query, binding_alias, params) when is_map(params) do
    select_merge(query, binding_alias, Map.to_list(params))
  end

  def select_merge(query, binding_alias, params) when is_list(params) do
    if Keyword.keyword?(params) do
      if Keyword.has_key?(params, :expression) do
        query_select_merge(query, binding_alias, params[:expression])
      else
        QueryHelpers.apply_expression(query, params, fn {key, value}, query ->
          query_select_merge(query, binding_alias, {key, value})
        end)
      end
    else
      query_select_merge(query, binding_alias, params)
    end
  end

  def select_merge(query, binding_alias, value) do
    query_select_merge(query, binding_alias, value)
  end

  defp query_select_merge(query, binding_alias, true) do
    if binding_alias do
      Query.select_merge(query, [{^binding_alias, q}], q)
    else
      Query.select_merge(query, [q], q)
    end
  end

  defp query_select_merge(query, binding_alias, value) do
    if binding_alias do
      Query.select_merge(query, [{^binding_alias, q}], ^value)
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

      iex> EctoShorts.CommonQueries.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments)>

      iex> EctoShorts.CommonQueries.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{as: :comments})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), as: :comments>

      iex> EctoShorts.CommonQueries.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{on: true})
      Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments)>

      iex> EctoShorts.CommonQueries.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{as: :comments, on: %{id: 2}})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), on: c1.id == ^2>

      iex> EctoShorts.CommonQueries.join(EctoShorts.Schema.Post, nil, EctoShorts.Schema.Post, :association, :comments, %{as: :comments, on: %{id: %{>=: 2}}})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, join: c1 in assoc(p0, :comments), on: c1.id >= ^2>
  """
  def join(query, binding_alias, join_type, key, params, opts) when is_map(params) do
    join(query, binding_alias, join_type, key, Map.to_list(params), opts)
  end

  def join(query, binding_alias, :association, key, params, opts) do
    schema_module = schema_module_for_query_expression!(query, binding_alias, params)

    qual = params[:qualifier] || :inner

    prefix = params[:prefix]

    as = params[:as]

    on =
      params
      |> Keyword.get(:on, true)
      |> join_on_dynamic(as, schema_module, opts)

    query_join_assoc(query, binding_alias, key, {as, qual, on, prefix})
  end

  def join(query, binding_alias, :subquery, subquery_data, params, opts) do
    schema_module = schema_module_for_query_expression!(query, binding_alias, params)

    qual = params[:qualifier] || :inner

    prefix = params[:prefix]

    as = params[:as]

    subquery_opts = params[:options] || []

    on =
      params
      |> Keyword.get(:on, true)
      |> join_on_dynamic(as, schema_module, opts)

    query_join_subquery(
      query,
      binding_alias,
      subquery_data,
      subquery_opts,
      {as, qual, on, prefix}
    )
  end

  defp query_join_assoc(query, binding_alias, key, {as, qual, on, prefix}) do
    if is_nil(as) or as === false do
      if binding_alias do
        Query.join(
          query,
          qual,
          [{^binding_alias, q}],
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
        if binding_alias do
          Query.join(
            query,
            qual,
            [{^binding_alias, q}],
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

  defp query_join_subquery(
         query,
         binding_alias,
         subquery_data,
         subquery_opts,
         {as, qual, on, prefix}
       ) do
    subquery = subquery(subquery_data, subquery_opts)

    if is_nil(as) or as === false do
      if binding_alias do
        Query.join(
          query,
          qual,
          [{^binding_alias, q}],
          ^subquery,
          on: ^on,
          prefix: ^prefix
        )
      else
        Query.join(
          query,
          qual,
          [q],
          ^subquery,
          on: ^on,
          prefix: ^prefix
        )
      end
    else
      Query.with_named_binding(query, as, fn query, as ->
        if binding_alias do
          Query.join(
            query,
            qual,
            [{^binding_alias, q}],
            ^subquery,
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        else
          Query.join(
            query,
            qual,
            [q],
            ^subquery,
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end)
    end
  end

  defp join_on_dynamic(true, _binding_alias, _schema_module, _opts) do
    true
  end

  defp join_on_dynamic(params, binding_alias, schema_module, opts) do
    dynamic(schema_module, binding_alias, params, opts)
  end

  def or_where(query, binding_alias, params, opts) when is_map(params) do
    or_where(query, binding_alias, Map.to_list(params), opts)
  end

  def or_where(query, binding_alias, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      apply_or_where(query, binding_alias, params, opts)
    else
      Enum.reduce(params, query, fn p, query ->
        or_where(query, binding_alias, p, opts)
      end)
    end
  end

  defp apply_or_where(query, binding_alias, params, opts) do
    if Keyword.has_key?(params, :expression) do
      query_or_where(query, binding_alias, params[:expression])
    else
      {schema_module, params} = Keyword.pop(params, :queryable)

      schema_module =
        with nil <- schema_module do
          query
          |> CommonQuery.to_query()
          |> CommonQuery.schema_module_for_query_expression!(binding_alias)
        end

      expr = dynamic(schema_module, binding_alias, params, opts)

      query_or_where(query, nil, expr)
    end
  end

  defp query_or_where(query, binding_alias, expr) do
    if binding_alias do
      Query.or_where(query, [{^binding_alias, q}], ^expr)
    else
      Query.or_where(query, [q], ^expr)
    end
  end

  def where(query, binding_alias, params, opts) when is_map(params) do
    where(query, binding_alias, Map.to_list(params), opts)
  end

  def where(query, binding_alias, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      apply_where(query, binding_alias, params, opts)
    else
      Enum.reduce(params, query, fn p, query ->
        where(query, binding_alias, p, opts)
      end)
    end
  end

  defp apply_where(query, binding_alias, params, opts) do
    if Keyword.has_key?(params, :expression) do
      query_where(query, binding_alias, params[:expression])
    else
      {schema_module, params} = Keyword.pop(params, :queryable)

      schema_module =
        with nil <- schema_module do
          query
          |> CommonQuery.to_query()
          |> CommonQuery.schema_module_for_query_expression!(binding_alias)
        end

      expr = dynamic(schema_module, binding_alias, params, opts)

      query_where(query, nil, expr)
    end
  end

  defp query_where(query, binding_alias, expr) do
    if binding_alias do
      Query.where(query, [{^binding_alias, q}], ^expr)
    else
      Query.where(query, [q], ^expr)
    end
  end

  defp schema_module_for_query_expression!(query, binding_alias, params) do
    if Keyword.has_key?(params, :queryable) do
      params[:queryable]
    else
      query
      |> CommonQuery.to_query()
      |> CommonQuery.schema_module_for_query_expression!(binding_alias)
    end
  end
end
