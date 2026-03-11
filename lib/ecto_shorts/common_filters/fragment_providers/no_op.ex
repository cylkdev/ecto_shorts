defmodule EctoShorts.CommonFilters.QueryProviders.NoOp do
  def build_fragment_expression(_selected_binding, _expression_key, _expression_params) do
    nil
  end
end
