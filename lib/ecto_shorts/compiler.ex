defmodule EctoShorts.Compiler do
  alias EctoShorts.Compiler.QueryBindingBuilder

  @spec query_binding_contracts(pos_integer(), atom()) :: Macro.t()
  def query_binding_contracts(positions, context) do
    QueryBindingBuilder.query_binding_contracts(positions, context)
  end
end
