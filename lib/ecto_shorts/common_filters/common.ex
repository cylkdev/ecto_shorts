defmodule EctoShorts.CommonFilters.Common do
  @filters [
    :preload,
    :start_date,
    :end_date,
    :before,
    :after,
    :ids,
    :first,
    :last,
    :limit,
    :offset,
    :search,
    :order_by
  ]

  def filters, do: @filters

  def build_query(query, _schema_module, _key, _value, _current_binding) do
    query
  end
end
