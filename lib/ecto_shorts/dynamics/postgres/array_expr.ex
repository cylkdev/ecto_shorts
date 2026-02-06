defmodule EctoShorts.Dynamics.Postgres.ArrayExpr do
  @moduledoc false

  alias EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr

  @doc false
  defdelegate apply_dynamic_expr(binding_selector, key, expr), to: ArrayExpr
end
