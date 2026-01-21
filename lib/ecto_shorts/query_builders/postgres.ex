defmodule EctoShorts.QueryBuilder.Postgres do
  @moduledoc false

  alias EctoShorts.QueryBuilder.{
    Joins,
    Filters,
    Selects
  }

  @behaviour EctoShorts.QueryBuilder

  @impl true
  def build_query(schema, :join, query, binding_selector, join_params, _opts) do
    Joins.build_query(schema, query, binding_selector, join_params)
  end

  def build_query(schema, :select, query, binding_selector, term, _opts) do
    Selects.build_query(schema, query, binding_selector, term)
  end

  def build_query(schema, filter, query, binding_selector, term, opts) do
    Filters.build_query(schema, filter || :where, query, binding_selector, term, opts)
  end
end
