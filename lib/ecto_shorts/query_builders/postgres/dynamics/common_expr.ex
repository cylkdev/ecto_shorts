defmodule EctoShorts.QueryBuilders.Dynamics.CommonExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilders.Dynamics.CommonExprBuilder

  require Ecto.Query
  require CommonExprBuilder

  CommonExprBuilder.define_common_exprs()
end
