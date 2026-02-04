defmodule EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.CommonExpr do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres

  @compile {:no_warn_undefined, {Postgres.Compiled.Common, :dynamic_field_expr, 3}}

  def dynamic_field_expr(binding_selector, key, expr) do
    Postgres.Compiled.Common.dynamic_field_expr(binding_selector, key, expr)
  end
end
