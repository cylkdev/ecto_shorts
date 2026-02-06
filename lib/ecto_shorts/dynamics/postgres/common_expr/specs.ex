defmodule EctoShorts.Dynamics.Postgres.CommonExpr.Specs do
  @moduledoc false

  alias EctoShorts.Dynamics.Adapters.Postgres.CommonExpr.Specs

  @doc false
  defdelegate clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts),
    to: Specs
end
