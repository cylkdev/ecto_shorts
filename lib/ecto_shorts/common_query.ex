defmodule EctoShorts.CommonQuery do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Provides functions for introspecting `Ecto.Query` structures at runtime.

  Use this module to resolve the source, prefix, binding count, or
  per-binding source of an `Ecto.Query`. Useful when composing queries
  dynamically or when debugging generated queries.

  ## Bindings

  Bindings can be resolved in two ways:

  * By **name** (the `:as` binding) - pass an atom matching the `as:` key
    used in `from` or `join`.
  * By **position** - pass an integer where `1` is the root `from` binding
    and each subsequent join increments the position.

  ## Source tuples

  Ecto represents sources as `{source, schema}` tuples:

      {"posts", MyApp.Post}    # schema module present
      {"posts", nil}            # bare table name, no schema

  Functions in this module return `nil` when a source or binding cannot be
  resolved.

  ## Getting started

      iex> import Ecto.Query
      ...> q = from p in EctoShorts.Schema.Post, as: :post
      ...> EctoShorts.CommonQuery.get_query_source(q)
      {"posts", EctoShorts.Schema.Post}

      iex> import Ecto.Query
      ...> q = from p in EctoShorts.Schema.Post, join: c in assoc(p, :comments), as: :comment
      ...> EctoShorts.CommonQuery.query_binding_count(q)
      2

  See also `EctoShorts.CommonSchema` and `EctoShorts.CommonFilters`.
  """

  alias Ecto.Queryable
  alias EctoShorts.SchemaHelpers

  @doc """
  Returns the query prefix for the given queryable, or `nil` if none is set.

  The prefix corresponds to the PostgreSQL schema (or equivalent) set via
  `Ecto.put_meta/2` or configured in the schema module.

  ## Examples

      iex> import Ecto.Query
      ...> q = from p in EctoShorts.Schema.PostHasSchemaPrefix
      ...> EctoShorts.CommonQuery.get_query_prefix(q)
      "custom_schema_prefix"

      iex> import Ecto.Query
      ...> q = from p in EctoShorts.Schema.Post
      ...> EctoShorts.CommonQuery.get_query_prefix(q)
      nil

  See also `get_query_source/1` and `EctoShorts.CommonSchema.get_schema_prefix/1`.
  """
  def get_query_prefix(queryable) do
    case queryable |> to_query!() |> get_query_source_expr() do
      %{prefix: prefix} -> prefix
      _ -> nil
    end
  end

  @doc """
  Returns the root source tuple for the given query.

  Traverses composed queries and subqueries until it reaches an expression
  with a concrete `{source, schema}` source tuple. Returns `nil` when the
  source cannot be resolved.

  ## Examples

      iex> import Ecto.Query
      ...> q = from p in "users"
      ...> EctoShorts.CommonQuery.get_query_source(q)
      {"users", nil}

      iex> import Ecto.Query
      ...> q = EctoShorts.Schema.User
      ...> EctoShorts.CommonQuery.get_query_source(q)
      {"users", EctoShorts.Schema.User}

      iex> import Ecto.Query
      ...> q = from {"custom_source", EctoShorts.Schema.User}
      ...> EctoShorts.CommonQuery.get_query_source(q)
      {"custom_source", EctoShorts.Schema.User}

  See also `get_query_prefix/1` and `get_query_binding_source/2`.
  """
  def get_query_source(queryable) do
    case queryable |> to_query!() |> get_query_source_expr() do
      %{source: source} -> source
      _ -> nil
    end
  end

  defp get_query_source_expr(%{query: query} = _subquery) do
    get_query_source_expr(query)
  end

  defp get_query_source_expr(%{from: from_expr} = _query) do
    get_query_source_expr(from_expr)
  end

  defp get_query_source_expr(%{source: {_, _}} = from_or_join_expr) do
    from_or_join_expr
  end

  defp get_query_source_expr(%{source: nil}) do
    nil
  end

  defp get_query_source_expr(%{source: source}) do
    get_query_source_expr(source)
  end

  defp get_query_source_expr(%_{}), do: nil

  defp get_query_source_expr(other) do
    other
    |> to_query!()
    |> get_query_source_expr()
  end

  @doc """
  Returns the number of bindings in the given query.

  Counts the root `from` binding as `1`, plus one for each join. Useful
  when building dynamic queries that need to stay within the binding limit
  configured via `:max_binding_positings`.

  ## Examples

      iex> EctoShorts.CommonQuery.query_binding_count(EctoShorts.Schema.Post)
      1

      iex> import Ecto.Query
      ...> q = from p in EctoShorts.Schema.Post, join: c in assoc(p, :comments), as: :comment
      ...> EctoShorts.CommonQuery.query_binding_count(q)
      2

  See also `get_query_binding_source/2` and `EctoShorts.Config.max_binding_positings/0`.
  """
  def query_binding_count(queryable) do
    query = to_query!(queryable)
    1 + length(query.joins)
  end

  @doc """
  Returns the source tuple for the given binding.

  `binding_alias_or_pos` can be a named binding atom (matching the `as:` key
  in the query) or an integer position (`1` for the root `from`, `2` for the
  first join, etc.).

  When the binding points to an association join (`assoc/2`), this function
  resolves the related schema from the parent binding and returns
  `{nil, RelatedSchema}`. Returns `nil` when the binding cannot be resolved.

  ## Examples

      iex> import Ecto.Query
      ...> q = from u in "users", as: :user
      ...> EctoShorts.CommonQuery.get_query_binding_source(q, :user)
      {"users", nil}

      iex> import Ecto.Query
      ...> q = from u in EctoShorts.Schema.User, as :user
      ...> EctoShorts.CommonQuery.get_query_binding_source(q, :user)
      {"users", EctoShorts.Schema.User}

      iex> import Ecto.Query
      ...> q = from u in EctoShorts.Schema.User, as: :user, join: p in assoc(u, :posts), as: :post
      ...> EctoShorts.CommonQuery.get_query_binding_source(q, :post)
      {nil, EctoShorts.Schema.Post}

      iex> import Ecto.Query
      ...> q = from u in EctoShorts.Schema.User, as: :user, join: p in assoc(u, :posts), as: :post
      ...> EctoShorts.CommonQuery.get_query_binding_source(q, 1)
      {"users", EctoShorts.Schema.User}

      iex> import Ecto.Query
      ...> q = from u in EctoShorts.Schema.User, as: :user, join: p in assoc(u, :posts), as: :post
      ...> EctoShorts.CommonQuery.get_query_binding_source(q, 2)
      {nil, EctoShorts.Schema.Post}

  See also `query_binding_count/1` and `get_query_source/1`.
  """
  def get_query_binding_source(queryable, binding_alias_or_pos) do
    case get_binding_expr(queryable, binding_alias_or_pos) do
      %Ecto.Query.JoinExpr{} = join_expr ->
        get_join_expr_source(queryable, join_expr)

      %Ecto.Query.FromExpr{source: source} ->
        source

      other ->
        other
    end
  end

  defp get_join_expr_source(%{from: from_expr}, %Ecto.Query.JoinExpr{assoc: {0, assoc_key}}) do
    case get_query_source(from_expr) do
      {_, nil} -> nil
      {_, schema} -> {nil, SchemaHelpers.get_related_schema(schema, assoc_key)}
    end
  end

  defp get_join_expr_source(
         %{joins: joins} = query,
         %Ecto.Query.JoinExpr{assoc: {index, assoc_key}} = join_expr
       ) do
    case get_in(joins, [Access.at(index)]) do
      %Ecto.Query.JoinExpr{assoc: {^index, ^assoc_key}} ->
        ref_join_expr = get_in(joins, [Access.at!(index - 1)])

        case get_join_expr_source(query, ref_join_expr) do
          {_, nil} -> nil
          {_, schema} -> {nil, SchemaHelpers.get_related_schema(schema, assoc_key)}
        end

      _ ->
        if has_subquery?(query) do
          query
          |> get_inner_query()
          |> get_join_expr_source(join_expr)
        end
    end
  end

  defp get_join_expr_source(_query, join_expr), do: get_query_source(join_expr)

  defp has_subquery?(%Ecto.Query{from: %Ecto.Query.FromExpr{source: %Ecto.SubQuery{}}}), do: true
  defp has_subquery?(%Ecto.Query{from: %{source: %Ecto.SubQuery{query: _}}}), do: true
  defp has_subquery?(%{source: %Ecto.SubQuery{query: _}}), do: true
  defp has_subquery?(_), do: false

  defp get_inner_query(%Ecto.Query{from: %{source: %{query: query}}}), do: query
  defp get_inner_query(_), do: nil

  defp get_binding_expr(%Ecto.Query{} = query, pos) when is_integer(pos) do
    binding_at(query, pos)
  end

  defp get_binding_expr(%Ecto.Query{} = query, binding_alias) do
    binding_for_alias(query, binding_alias)
  end

  defp get_binding_expr(source, binding_alias_or_pos) do
    source
    |> Queryable.to_query()
    |> get_binding_expr(binding_alias_or_pos)
  end

  defp binding_at(%Ecto.Query{from: from_expr} = _query, 1) do
    from_expr
  end

  defp binding_at(%Ecto.Query{joins: joins} = _query, pos) when pos < 0 do
    Enum.at(joins, pos)
  end

  defp binding_at(%Ecto.Query{joins: joins} = _query, pos) when pos > 1 do
    Enum.at(joins, pos - 2)
  end

  defp binding_for_alias(query, binding_alias) do
    case get_named_binding_expr(query, binding_alias) do
      %Ecto.Query.FromExpr{} = from_expr ->
        get_query_source_expr(from_expr)

      %Ecto.Query.JoinExpr{} = join_expr ->
        join_expr

      _ ->
        nil
    end
  end

  defp get_named_binding_expr(%{from: from_expr, joins: joins} = query, binding_alias)
       when is_struct(query, Ecto.Query) do
    if is_nil(binding_alias) do
      get_query_source_expr(from_expr)
    else
      with nil <- get_named_binding_expr(from_expr, binding_alias) do
        get_named_binding_expr(joins, binding_alias)
      end
    end
  end

  defp get_named_binding_expr(%Ecto.SubQuery{query: query}, binding_alias) do
    get_named_binding_expr(query, binding_alias)
  end

  defp get_named_binding_expr(
         %Ecto.Query.JoinExpr{as: binding_alias, source: _} = join_expr,
         binding_alias
       ),
       do: join_expr

  defp get_named_binding_expr(%Ecto.Query.JoinExpr{source: source}, binding_alias),
    do: get_named_binding_expr(source, binding_alias)

  defp get_named_binding_expr(
         %Ecto.Query.FromExpr{as: binding_alias, source: _} = from_expr,
         binding_alias
       ),
       do: from_expr

  defp get_named_binding_expr(%Ecto.Query.FromExpr{source: source}, binding_alias),
    do: get_named_binding_expr(source, binding_alias)

  defp get_named_binding_expr([], _binding_alias), do: nil

  defp get_named_binding_expr([join_expr | joins], binding_alias) do
    with nil <- get_named_binding_expr(join_expr, binding_alias) do
      get_named_binding_expr(joins, binding_alias)
    end
  end

  defp get_named_binding_expr(_, _), do: nil

  defp to_query!(queryable) do
    case queryable do
      %_{} = query ->
        query

      other when is_atom(other) or is_binary(other) or is_tuple(other) ->
        Queryable.to_query(other)

      term ->
        raise ArgumentError, "expected a queryable, got: #{inspect(term)}"
    end
  end
end
