defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.CommonExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.CommonExprBuilder

  require Ecto.Query
  require CommonExprBuilder

  CommonExprBuilder.define_common_exprs()
end
