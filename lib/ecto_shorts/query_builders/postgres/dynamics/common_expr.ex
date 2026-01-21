defmodule EctoShorts.QueryBuilder.Dynamics.CommonExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Dynamics.CommonExprBuilder

  require Ecto.Query
  require CommonExprBuilder

  CommonExprBuilder.define_common_exprs()
end
