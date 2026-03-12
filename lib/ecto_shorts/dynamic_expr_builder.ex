defmodule EctoShorts.DynamicExprBuilder do
  @type dynamic_expr :: %Ecto.Query.DynamicExpr{}

  @type source :: term()
  @type selected_binding :: {:as, atom()} | {:at, pos_integer()}
  @type input :: term()
  @type opts :: keyword()

  @callback build_dynamic(source, selected_binding, input, opts) :: dynamic_expr()
end
