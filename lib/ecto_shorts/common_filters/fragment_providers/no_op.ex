defmodule EctoShorts.CommonFilters.QueryProviders.NoOp do
  def resolve_query_expression(_selected_binding, _expression_key, _expression_params) do
    nil
  end
end
