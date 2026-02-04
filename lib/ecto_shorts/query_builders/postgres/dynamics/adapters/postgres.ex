defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Adapters.Postgres do
  @moduledoc false

  use EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseAdapter

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.Emitters.DynamicFieldExpr

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.{
    ArrayExprBuilder,
    CommonExprBuilder,
    ScalarExprBuilder
  }

  def emitter_module, do: DynamicFieldExpr

  def options, do: [max_positional_bindings: 10]

  def clause_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
    CommonExprBuilder.clause_specs(
      kind,
      context,
      binding_head_ast,
      target_binding_var,
      binding_body_asts
    ) ++
      ScalarExprBuilder.clause_specs(
        kind,
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
      ArrayExprBuilder.clause_specs(
        kind,
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )
  end
end
