defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Specs do
  @moduledoc false

  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs

  @doc false
  defdelegate clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts),
    to: Specs

  @doc false
  defdelegate list_semantic_specs(context, binding_head_ast), to: Specs

  @doc false
  defdelegate alias_op_specs(context, binding_head_ast), to: Specs

  @doc false
  defdelegate nil_specs(context, binding_head_ast, target_binding_var, binding_body_asts),
    to: Specs

  @doc false
  defdelegate lower_upper_specs(context, binding_head_ast, target_binding_var, binding_body_asts),
    to: Specs

  @doc false
  defdelegate like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts),
    to: Specs

  @doc false
  defdelegate base_op_specs(context, binding_head_ast, target_binding_var, binding_body_asts),
    to: Specs
end
