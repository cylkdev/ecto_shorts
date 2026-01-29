defmodule EctoShorts.QueryBuilder.Postgres.Dynamics.ScalarExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Postgres.Dynamics.ScalarExprBuilder

  require Ecto.Query
  require ScalarExprBuilder

  ScalarExprBuilder.define_scalar_exprs()
end
