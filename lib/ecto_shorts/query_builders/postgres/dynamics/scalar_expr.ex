defmodule EctoShorts.QueryBuilders.Dynamics.ScalarExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilders.Dynamics.ScalarExprBuilder

  require Ecto.Query
  require ScalarExprBuilder

  ScalarExprBuilder.define_scalar_exprs()
end
