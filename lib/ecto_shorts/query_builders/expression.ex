defmodule EctoShorts.QueryBuilder.Expression do
  @moduledoc since: "2.5.0"
  @moduledoc """
  This module provides wrapper functions that simplifies the usage of the `Ecto.Query` api.
  """

  alias Ecto.Query

  require Ecto.Query

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

  def put_query_prefix(query, prefix) do
    Query.put_query_prefix(query, prefix)
  end

  def subquery(query, opts \\ []) do
    Query.subquery(query, opts)
  end

  def exclude(query, field) do
    Query.exclude(query, field)
  end

  def limit(query, current_binding, value) do
    if current_binding do
      Query.limit(query, [{^current_binding, q}], ^value)
    else
      Query.limit(query, [q], ^value)
    end
  end

  def offset(query, current_binding, value) do
    if current_binding do
      Query.offset(query, [{^current_binding, q}], ^value)
    else
      Query.offset(query, [q], ^value)
    end
  end

  def group_by(query, current_binding, value) do
    if current_binding do
      Query.group_by(query, [{^current_binding, q}], ^value)
    else
      Query.group_by(query, [q], ^value)
    end
  end

  def order_by(query, current_binding, value) do
    if current_binding do
      Query.order_by(query, [{^current_binding, q}], ^value)
    else
      Query.order_by(query, [q], ^value)
    end
  end

  def preload(query, current_binding, value) do
    if current_binding do
      Query.preload(query, [{^current_binding, q}], ^value)
    else
      Query.preload(query, [q], ^value)
    end
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

  @default_join_opts [
    qualifier: :inner,
    on: true
  ]

  def join(query, current_binding, {type, key, opts}) when is_map(opts) do
    join(query, current_binding, {type, key, Map.to_list(opts)})
  end

  def join(query, {current_binding, join_binding_alias}, {:association, key, opts}) do
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

  def join(query, current_binding, {:association, key, opts}) do
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

  def join(query, {current_binding, join_binding_alias}, {:subquery, from, opts}) do
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

  def join(query, current_binding, {:subquery, from, opts}) do
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

  def or_where(query, current_binding, value) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], ^value)
    else
      Query.or_where(query, [q], ^value)
    end
  end

  def where(query, current_binding, value) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], ^value)
    else
      Query.where(query, [q], ^value)
    end
  end
end
