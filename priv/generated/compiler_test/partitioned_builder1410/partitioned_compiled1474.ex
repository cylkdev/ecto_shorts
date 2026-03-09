defmodule EctoShorts.CompilerTest.PartitionedCompiled1474 do
  @moduledoc false

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr(selected_binding, key, value) do
    case EctoShorts.CompilerTest.PartitionedCompiled1474.Partition1.dynamic_expr(selected_binding, key, value) do
      nil ->
        case EctoShorts.CompilerTest.PartitionedCompiled1474.Partition2.dynamic_expr(
               selected_binding,
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
