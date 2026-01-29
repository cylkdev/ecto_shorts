defmodule EctoShorts.QueryBuilders.Postgres do
  @moduledoc false

  alias EctoShorts.QueryBuilder.{
    Joins,
    Filters,
    Selects
  }

  @behaviour EctoShorts.QueryBuilder

  @impl true
  def build_query(source, query, binding_selector, current_filter, args, opts) do
    case current_filter do
      :join -> Joins.build_query(source, query, binding_selector, args)
      :select -> Selects.build_query(source, query, binding_selector, args)
      _ -> Filters.build_query(source, current_filter, query, binding_selector, args, opts)
    end
  end
end
