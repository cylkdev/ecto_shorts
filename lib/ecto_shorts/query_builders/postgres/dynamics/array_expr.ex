defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExpr do
  @moduledoc false
  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Adapters.Postgres

  @compile {:no_warn_undefined, {Postgres.Compiled.Array, :dynamic_field_expr, 3}}

  def dynamic_field_expr(binding_selector, key, expr) do
    Postgres.Compiled.Array.dynamic_field_expr(binding_selector, key, expr)
  end
end
