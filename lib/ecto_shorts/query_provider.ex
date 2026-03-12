defmodule EctoShorts.QueryProvider do
  def resolve_query_expression(module, selected_binding, expression_key, expression_params, opts) do
    module.resolve_query_expression(selected_binding, expression_key, expression_params)
  end
end
