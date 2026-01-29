defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.ScalarExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.ScalarExprBuilder

  require Ecto.Query
  require ScalarExprBuilder

  ScalarExprBuilder.define_scalar_exprs()
end
