defmodule EctoShorts.CommonQuery do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides functions for introspecting `Ecto.Query` structures.

  This module is useful when you need to inspect a query and determine which
  schema (or source) is associated with a given binding.

  ## Bindings

  Bindings can be resolved in two ways:

    * By **name** (the `:as` binding), when the query uses `as: :name`
    * By **position**, where `1` is the `from` binding and positive integers
      above `1` address joins

  ## Source tuples

  Ecto represents sources as `{source, schema}` tuples, such as:

      {"posts", MyApp.Post}
      {"posts", nil}

  where `schema` is `nil` when the query is built from a bare table name.
  """

  alias Ecto.Queryable
  alias EctoShorts.SchemaHelpers

  @doc """
  Returns the query prefix for the given queryable, or `nil` if none is set.
  """
  def get_query_prefix(queryable) do
    case queryable |> to_query!() |> get_query_source_expr() do
      %{prefix: prefix} -> prefix
      _ -> nil
    end
  end

  @doc """
  Returns the root source tuple for the given query.

  This function traverses composed queries and subqueries until it reaches an
  expression with a concrete source tuple.

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

  This counts the root `from` binding as `1`, plus one binding per join.
  """
  def query_binding_count(queryable) do
    query = to_query!(queryable)
    1 + length(query.joins)
  end

  @doc """
  Returns the source tuple for the given binding.

  If the binding points to an association join (for example, `assoc(p, :comments)`),
  this function attempts to resolve the related schema and returns `{nil, Schema}`.

  If the binding cannot be resolved, returns `nil`.

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
