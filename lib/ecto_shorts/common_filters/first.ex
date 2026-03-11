defmodule EctoShorts.CommonFilters.First do
  alias EctoShorts.CommonFilters.Limit

  def build_query(:first, source, query, selected_binding, expr, opts) do
    Limit.build_query(:limit, source, query, selected_binding, expr, opts)
  end
end
