defmodule EctoShorts.Compiler.ClauseBuilder do
  @moduledoc """
  Builds quoted `apply_dynamic_expr/3` clauses from validated clause specs.
  """

  alias EctoShorts.Compiler.ClauseSpec

  @doc """
  Builds a quoted `apply_dynamic_expr/3` clause from a clause spec.

  ## Error reasons

    * `:missing_key` - a required spec key is missing (from `ClauseSpec.new/1`)
    * `:invalid_spec` - the spec attrs are not valid (from `ClauseSpec.new/1`)
  """
  @spec clause_ast(ClauseSpec.t() | map() | keyword()) :: {:ok, Macro.t()} | {:error, term()}
  def clause_ast(spec) do
    with {:ok, spec} <- ClauseSpec.new(spec) do
      {:ok,
       quote_def(
         spec.binding_head,
         spec.key,
         spec.head,
         spec.body,
         spec.guard
       )}
    end
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
