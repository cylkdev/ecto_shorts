defmodule EctoShorts.QueryBuilder.Adapters.Postgres do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Stages.{Filters, Joins, Selects}

  @behaviour EctoShorts.QueryBuilder.Adapter

  @common_filters [:ids, :before, :after, :start_date, :end_date]

  @pagination_filters [
    :first,
    :last,
    :limit,
    :offset,
    :order_by,
    :preload
  ]

  @query_api_filters [
    :join,
    :select,
    :select_merge
  ]

  @filters @common_filters ++ @pagination_filters ++ @query_api_filters

  @doc "Returns the list of supported filter keys for this adapter."
  def filters, do: @filters

  @impl true
  def build_query(source, query, binding_selector, :join, args, _opts) do
    Joins.build(source, query, binding_selector, args)
  end

  def build_query(source, query, binding_selector, filter, args, _opts)
      when filter in [:select, :select_merge] do
    Selects.build(filter, source, query, binding_selector, args)
  end

  def build_query(source, query, binding_selector, filter, args, opts) do
    Filters.build(source, filter, query, binding_selector, args, opts)
  end
end
