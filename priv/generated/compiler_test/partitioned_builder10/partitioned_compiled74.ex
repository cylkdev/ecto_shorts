defmodule EctoShorts.CompilerTest.PartitionedCompiled74 do
  @moduledoc false

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr(binding_selector, key, value) do
    case EctoShorts.CompilerTest.PartitionedCompiled74.Partition1.dynamic_expr(binding_selector, key, value) do
      nil ->
        case EctoShorts.CompilerTest.PartitionedCompiled74.Partition2.dynamic_expr(
               binding_selector,
               key,
               value
             ) do
          nil -> nil
          result -> result
        end

      result ->
        result
    end
  end
end