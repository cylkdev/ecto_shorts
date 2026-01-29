defmodule EctoShorts.QueryBuilder.Postgres.Pagination do
  @moduledoc false
  alias Ecto.Query

  require Ecto.Query

  @filters [:first, :last, :limit, :offset, :order_by, :preload]

  def filters, do: @filters

  def build_query(_schema, query, :first, limit_value) do
    limit(query, limit_value)
  end

  def build_query(_schema, query, :last, {sort_key, limit_value}) do
    last(query, sort_key, limit_value)
  end

  def build_query(_schema, query, :last, limit_value) do
    last(query, :id, limit_value)
  end

  def build_query(_schema, query, :limit, limit_value) do
    limit(query, limit_value)
  end

  def build_query(_schema, query, :offset, offset_value) do
    offset(query, offset_value)
  end

  def build_query(_schema, query, :order_by, order_spec) do
    order_by(query, order_spec)
  end

  def build_query(_schema, query, :preload, preload_spec) do
    preload(query, preload_spec)
  end

  def offset(query, offset_value) do
    Query.offset(query, ^offset_value)
  end

  def limit(query, limit_value) do
    Query.limit(query, ^limit_value)
  end

  def last(query, sort_key, limit_value) do
    query
    |> Query.exclude(:order_by)
    |> Query.from(order_by: [desc: ^sort_key], limit: ^limit_value)
    |> Query.subquery()
    |> Query.order_by(^sort_key)
  end

  def order_by(query, order_spec) do
    normalized_spec = normalize_to_list(order_spec)
    Query.order_by(query, ^normalized_spec)
  end

  def preload(query, preload_spec) do
    normalized_spec = normalize_to_list(preload_spec)
    Query.preload(query, ^normalized_spec)
  end

  defp normalize_to_list(map_value) when is_map(map_value), do: Map.to_list(map_value)
  defp normalize_to_list(value), do: value
end
