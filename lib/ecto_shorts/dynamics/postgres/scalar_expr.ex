defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  @moduledoc false

  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr

  @doc false
  defdelegate apply_dynamic_expr(binding_selector, key, expr), to: ScalarExpr
end
