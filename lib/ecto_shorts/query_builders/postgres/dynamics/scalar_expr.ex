defmodule EctoShorts.QueryBuilder.Dynamics.ScalarExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Dynamics.ScalarExprBuilder

  require Ecto.Query
  require ScalarExprBuilder

  ScalarExprBuilder.define_scalar_exprs()
end
