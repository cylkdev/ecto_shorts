defmodule EctoShorts.CommonQuery do
  @moduledoc since: "2.5.0"
  @moduledoc """
  `EctoShorts.CommonQuery` provides helper functions for introspecting Ecto queries.
  """

  alias Ecto.Queryable
  alias EctoShorts.SchemaHelpers

  @type subquery :: Ecto.SubQuery.t()
  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type source_and_schema :: {schema_source(), schema_module()}
  @type sourceable :: schema_module() | source_and_schema()
  @type query_source :: query() | sourceable()
  @type from_expr :: %Ecto.Query.FromExpr{}
  @type join_expr :: %Ecto.Query.JoinExpr{}
  @type binding_alias :: atom()

  @type key() :: atom()

  @doc """
  Converts the given `schema_module`, or `{schema_source, schema_module}`
  to an Ecto.Query struct or returns an `Ecto.Query` struct as-is.

  ## Examples

      iex> EctoShorts.CommonQuery.to_query(EctoShorts.Schema.Post)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post>

      iex> EctoShorts.CommonQuery.to_query({"custom_source", EctoShorts.Schema.Post})
      #Ecto.Query<from p0 in {"custom_source", EctoShorts.Schema.Post}>

      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.to_query(query)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post>
  """
  @spec to_query(any()) :: query()
  def to_query(query) when is_struct(query, Ecto.Query), do: query
  def to_query(source), do: Queryable.to_query(source)

  def get_from_expr(query, key) do
    with from_expr when is_map(from_expr) <- get_from_expr(query) do
      Map.get(from_expr, key)
    end
  end

  @doc """
  Returns the root `from` expression from a query, subquery, or schema.

  Ecto queries can be composed using different layers like schemas,
  subqueries, or pre-built query structs. This function walks through
  any of those forms and returns the final `%Ecto.Query.FromExpr{}`
  that describes the base table and schema.

  In an `Ecto.Query.FromExpr`, the `:source` field can be one of the
  following types:

  1. `{binary(), queryable()} Tuple`

  This is the most common form:

      {"posts", MyApp.Post}

  The binary represents the table name (e.g. "posts").

  The second element is a `queryable`, typically a schema module like
  `MyApp.Post`.

  This format appears when you build queries directly from schema
  modules or explicitly specify a source.

  2. An Ecto.Query.t()

  This can happen when you pass a base query directly into another
  from expression:

      query = from p in Post, where: p.published == true
      outer = from p in query

  Here, `outer.from.source` will hold the inner Ecto.Query struct.

  3. An %Ecto.SubQuery{} Struct

  This occurs when you wrap a query with subquery/1:

      inner = from p in Post, where: p.published == true
      outer = from p in subquery(inner), as: :post

  In this case, `outer.from.source` will be a %Ecto.SubQuery{} struct.
  """
  @spec get_from_expr(any()) :: from_expr()
  def get_from_expr(%{source: {_, _}} = from_expr), do: from_expr
  def get_from_expr(%{source: source}), do: get_from_expr(source)
  def get_from_expr(%{from: from_expr}), do: get_from_expr(from_expr)
  def get_from_expr(%{query: query} = _subquery), do: get_from_expr(query)
  def get_from_expr(query_source), do: query_source |> to_query() |> get_from_expr()

  @doc """
  Checks if a query is ultimately built on top of a subquery.

  When working with Ecto, you can create queries using a schema, another query,
  or a subquery. This function helps you figure out if a given query is backed
  by a subquery.

  It works by looking at the source of the query and recursively following
  any nested queries until it reaches the base. If that base is a subquery,
  it returns `true`. Otherwise, it returns `false`.

  ## Examples

      # When using a schema directly

      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.has_source_subquery?(query)
      false

      # When using a subquery as the base

      iex> import Ecto.Query
      ...> base = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> query = from p in subquery(base), as: :post
      ...> EctoShorts.CommonQuery.has_source_subquery?(query)
      true

      ## When wrapping a subquery multiple times

      iex> import Ecto.Query
      ...> first_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> second_query = from p in subquery(first_query), as: :post
      ...> third_query = from p in second_query, where: p.id in [1, 2, 3]
      ...> EctoShorts.CommonQuery.has_source_subquery?(third_query)
      true

      # When nesting queries without using `subquery/1`

      iex> import Ecto.Query
      ...> base = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> query = from p in base, where: p.id in [1, 2, 3]
      ...> EctoShorts.CommonQuery.has_source_subquery?(query)
      false
  """
  @spec has_source_subquery?(any()) :: boolean()
  def has_source_subquery?(%{source: %{query: _}}), do: true
  def has_source_subquery?(%{from: from}), do: has_source_subquery?(from)
  def has_source_subquery?(%{query: query}), do: has_source_subquery?(query)
  def has_source_subquery?(_), do: false

  @doc """
  Looks up a value from the subquery used as the source for the given query.

  This function first checks if the query is based on a subquery (using
  `get_source_subquery/1`), and if so, retrieves the value for the given
  `key` from that subquery.

  Returns `nil` if the query does not use a subquery as its source.

  ## Examples

      # Access the original `:from` clause of the subquery
      iex> import Ecto.Query
      ...> inner_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> outer_query = from p in subquery(inner_query), as: :post
      ...> EctoShorts.CommonQuery.get_source_subquery(outer_query, :query)
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.published == true>
  """
  @spec get_source_subquery(any(), atom()) :: any() | nil
  def get_source_subquery(query, key) do
    with subquery when is_map(subquery) <- get_source_subquery(query) do
      Map.get(subquery, key)
    end
  end

  @doc """
  Returns the first subquery found in the query's source chain.

  This function checks whether a query or subquery is being used as the
  source of a query (instead of a schema or table). It walks through the
  nested structure of the query to find the original subquery if present.

  It’s useful when you want to introspect deeply nested queries and extract
  the original data source they’re built on top of.

  ## Examples

      # When the source is a schema (not a subquery)
      iex> import Ecto.Query
      ...> EctoShorts.CommonQuery.get_source_subquery(EctoShorts.Schema.Post)
      nil

      # A query that directly wraps a subquery
      iex> import Ecto.Query
      ...> source_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> outer_query = from p in subquery(source_query), as: :post
      ...> EctoShorts.CommonQuery.get_source_subquery(outer_query)
      subquery(source_query)

      # A query that wraps a subquery inside another query
      iex> import Ecto.Query
      ...> source_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> outer_query = from p in subquery(source_query), as: :post
      ...> final_query = from p in outer_query, where: p.id in [1, 2, 3]
      ...> EctoShorts.CommonQuery.get_source_subquery(final_query)
      subquery(source_query)

      # A deeply nested query containing a subquery
      iex> import Ecto.Query
      ...> base_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> source_query = from p in subquery(base_query), as: :post
      ...> outer_query = from p in subquery(source_query), where: p.id in [1, 2, 3]
      ...> final_query = from p in outer_query, where: p.id in [1, 2, 3]
      ...> EctoShorts.CommonQuery.get_source_subquery(final_query)
      subquery(source_query)
  """
  @spec get_source_subquery(any()) :: subquery() | nil
  def get_source_subquery(%{source: {_, _}}), do: nil
  def get_source_subquery(%{source: %{query: _} = subquery}), do: subquery
  def get_source_subquery(%{source: source}), do: get_source_subquery(source)
  def get_source_subquery(%{from: from}), do: get_source_subquery(from)
  def get_source_subquery(%{query: query}), do: get_source_subquery(query)
  def get_source_subquery(_), do: nil

  def find_binding_expr_source_and_schema(query, binding_alias) do
    case find_binding_expr(query, binding_alias) |> IO.inspect(label: "1") do
      nil ->
        nil

      %{on: _, source: source} = join_expr ->
        if has_source_subquery?(join_expr) do
          join_expr
          |> get_source_subquery(:query)
          |> get_from_expr(:source)
          |> elem(1)
          |> find_binding_expr_source_and_schema(binding_alias)
        else
          ensure_source_tuple(source)
        end

      %{source: source} = expr ->
        if has_source_subquery?(expr) do
          expr
          |> get_source_subquery(:query)
          |> get_from_expr(:source)
          |> elem(1)
          |> find_binding_expr_source_and_schema(binding_alias)
        else
          ensure_source_tuple(source)
        end

      {%{assoc: {_binding_position, assoc_key}} = _join_expr, from_expr} ->
        {_schema_source, schema_module} = from_expr.source

        assoc_schema_module =
          SchemaHelpers.get_association_schema_module(
            schema_module,
            assoc_key
          )

        {assoc_schema_module.__schema__(:source), assoc_schema_module}

      {%{on: _, source: source} = join_expr, _from_expr} ->
        if has_source_subquery?(join_expr) do
          join_expr
          |> get_source_subquery(:query)
          |> get_from_expr(:source)
          |> elem(1)
          |> find_binding_expr_source_and_schema(binding_alias)
        else
          ensure_source_tuple(source)
        end
    end
  end

  defp ensure_source_tuple({_, _} = tuple), do: tuple

  @doc """
  Finds the query expression that corresponds to a given binding alias
  in a query or subquery.

  This function is useful when you want to retrieve the underlying Ecto
  query expression (such as a `from` or `join`) associated with a specific
  named binding in your query. It works with nested subqueries and joins,
  traversing through them to find the expression that matches the binding
  alias.

  If a join expression is returned, the result is a tuple of the join
  expression and the original `from` expression it joins onto.

  If the match is found in the base `from`, only that expression is
  returned.

  ## Examples

      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> EctoShorts.CommonQuery.find_binding_expr(query, nil)

      iex> import Ecto.Query
      ...> base_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> query = from p in subquery(base_query), as: :post, where: p.id == 1
      ...> EctoShorts.CommonQuery.find_binding_expr(query, :post)

      iex> import Ecto.Query
      ...> posts_query = from p in EctoShorts.Schema.Post, where: p.published == true
      ...> comments_query =
      ...>   from c in EctoShorts.Schema.Comment,
      ...>     join: p in subquery(posts_query),
      ...>     as: :comments,
      ...>     on: c.post_id == p.id
      ...> EctoShorts.CommonQuery.find_binding_expr(comments_query, :comments)

      iex> import Ecto.Query
      ...> query =
      ...>   from p in EctoShorts.Schema.Post,
      ...>     as: :post,
      ...>     join: assoc(p, :comments),
      ...>     as: :comments,
      ...>     on: true
      ...> EctoShorts.CommonQuery.find_binding_expr(query, :comments)
  """
  @spec find_binding_expr(
          query()
          | subquery()
          | list(join_expr()),
          atom() | nil
        ) :: from_expr() | join_expr() | {join_expr(), from_expr()} | nil
  def find_binding_expr([], _binding_alias) do
    nil
  end

  def find_binding_expr([head | tail] = _joins, binding_alias) do
    with nil <- find_binding_expr(head, binding_alias) do
      find_binding_expr(tail, binding_alias)
    end
  end

  def find_binding_expr(%{from: from_expr, joins: joins}, binding_alias) do
    with nil <- find_binding_expr(from_expr, binding_alias) do
      with %{on: _, as: _} = join_expr <-
             find_binding_expr(joins, binding_alias) do
        {join_expr, from_expr}
      end
    end
  end

  def find_binding_expr(%{as: as, on: _} = join_expr, binding_alias) do
    if as === binding_alias do
      join_expr
    else
      if has_source_subquery?(join_expr) do
        join_expr
        |> get_source_subquery(:query)
        |> find_binding_expr(binding_alias)
      end
    end
  end

  def find_binding_expr(%{as: as} = from_expr, binding_alias) do
    if as === binding_alias do
      from_expr
    else
      if has_source_subquery?(from_expr) do
        from_expr
        |> get_source_subquery(:query)
        |> find_binding_expr(binding_alias)
      end
    end
  end

  def find_binding_expr(query_source, binding_alias) do
    query_source
    |> to_query()
    |> find_binding_expr(binding_alias)
  end

  # @doc """
  # ...
  # """
  # def fetch_binding_expr_source_schema_source!(query, binding_alias) do
  #   {schema_source, _schema_module} = fetch_binding_expr_source!(query, binding_alias)

  #   schema_source
  # end

  # @doc """
  # ...
  # """
  # def fetch_binding_expr_source_schema_module!(query, binding_alias) do
  #   {_schema_source, schema_module} = fetch_binding_expr_source!(query, binding_alias)

  #   schema_module
  # end

  # @doc """
  # ...
  # """
  # @spec fetch_binding_expr_source!(query(), binding_alias()) :: schema_module() | :error
  # def fetch_binding_expr_source!(query, binding_alias) do
  #   with :error <- fetch_binding_expr_source(query, binding_alias) do
  #     raise ArgumentError,
  #           "named binding alias '#{inspect(binding_alias)}' not found: #{inspect(query)}"
  #   end
  # end

  # @doc """
  # ...
  # """
  # @spec fetch_binding_expr_source(query(), binding_alias()) :: schema_module() | :error
  # def fetch_binding_expr_source(query, binding_alias) do
  #   with nil <- find_binding_expr_source(query, binding_alias) do
  #     :error
  #   end
  # end

  # @doc """
  # Returns the source associated with a named binding in an Ecto query.

  # ## Examples

  # """
end
