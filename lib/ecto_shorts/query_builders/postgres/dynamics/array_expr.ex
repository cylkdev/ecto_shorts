defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExpr do
  @moduledoc false
  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ArrayExprBuilder

  require Ecto.Query
  require ArrayExprBuilder

  ArrayExprBuilder.define_exprs()
end
