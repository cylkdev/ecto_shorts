defmodule EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres do
  @moduledoc false
  use EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter

  alias EctoShorts.QueryBuilder.Dynamics.Expression.Emitters.DynamicFieldExpr

  alias EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.Specs.{
    ArrayExprs,
    CommonExprs,
    ScalarExprs
  }

  def emitter_module, do: DynamicFieldExpr

  def options, do: [max_positional_bindings: 10]

  def clause_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
    CommonExprs.clause_specs(
      kind,
      context,
      binding_head_ast,
      target_binding_var,
      binding_body_asts
    ) ++
      ScalarExprs.clause_specs(
        kind,
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
      ArrayExprs.clause_specs(
        kind,
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )
  end
end
