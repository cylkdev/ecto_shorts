defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  @moduledoc false

  alias EctoShorts.Dynamics.Adapters.Postgres.CommonExpr

  @doc false
  defdelegate apply_dynamic_expr(binding_selector, key, expr), to: CommonExpr
end
