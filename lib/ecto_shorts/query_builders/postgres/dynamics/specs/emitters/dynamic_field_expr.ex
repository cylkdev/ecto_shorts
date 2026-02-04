defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.Emitters.DynamicFieldExpr do
  @moduledoc """
  Emits `def dynamic_field_expr/3` clauses from clause specs.

  This emitter matches the existing `ClauseBuilder` output.
  """

  @behaviour EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseEmitter

  @impl true
  def quoted_def(_kind, binding_head_ast, key_ast, head_ast, body_ast, nil) do
    quote do
      def dynamic_field_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast)) do
        unquote(body_ast)
      end
    end
  end

  def quoted_def(_kind, binding_head_ast, key_ast, head_ast, body_ast, guard_ast) do
    quote do
      def dynamic_field_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast))
          when unquote(guard_ast) do
        unquote(body_ast)
      end
    end
  end
end

