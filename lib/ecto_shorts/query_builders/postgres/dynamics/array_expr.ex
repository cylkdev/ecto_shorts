defmodule EctoShorts.QueryBuilders.Dynamics.ArrayExpr do
  @moduledoc false
  alias EctoShorts.QueryBuilders.Dynamics.ArrayExprBuilder

  require Ecto.Query
  require ArrayExprBuilder

  ArrayExprBuilder.define_nil_comparisons()
  ArrayExprBuilder.define_case_transforms()
  ArrayExprBuilder.define_like_ilikes()
  ArrayExprBuilder.define_base_ops()
end
