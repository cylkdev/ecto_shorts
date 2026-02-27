defmodule EctoShorts.Compiler.ClauseBuilder do
  @moduledoc """
  Builds quoted `apply_dynamic_expr/3` clauses from validated clause specs.
  """

  alias EctoShorts.Compiler.ClauseSpec

  @doc """
  Builds a quoted `apply_dynamic_expr/3` clause from a clause spec.
  """
  @spec clause_ast(ClauseSpec.t() | map() | keyword()) :: Macro.t()
  def clause_ast(attrs) do
    spec = ClauseSpec.new(attrs)

    quote_def(
      spec.binding_head,
      spec.key,
      spec.head,
      spec.body,
      spec.guard
    )
  end

  defp quote_def(binding_head_ast, key_ast, head_ast, body_ast, nil) do
    quote do
      def apply_dynamic_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast)) do
        unquote(body_ast)
      end
    end
  end

  defp quote_def(binding_head_ast, key_ast, head_ast, body_ast, guard_ast) do
    quote do
      def apply_dynamic_expr(unquote(binding_head_ast), unquote(key_ast), unquote(head_ast))
          when unquote(guard_ast) do
        unquote(body_ast)
      end
    end
  end
end
