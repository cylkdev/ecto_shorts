defmodule EctoShorts.CommonFiltersRc.Common do
  @moduledoc """
  ...
  """

  # alias EctoShorts.CommonFiltersRc.QueryExpression

  @filters ~w(
    preload
    start_date
    end_date
    before
    after
    ids
    first
    last
    limit
    offset
    search
    order_by
  )a

  def filters, do: @filters

  def build_query_expression(query, _schema_module, {_key, _value}, _current_binding) do
    query
  end
end
