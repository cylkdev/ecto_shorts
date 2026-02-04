defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.ScalarExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ScalarExprBuilder

  require Ecto.Query
  require ScalarExprBuilder

  ScalarExprBuilder.define_exprs()
end
