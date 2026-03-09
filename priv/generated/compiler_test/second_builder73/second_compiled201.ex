defmodule EctoShorts.CompilerTest.SecondCompiled201 do
  @moduledoc false

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr(selected_binding, key, value) do
    case EctoShorts.CompilerTest.SecondCompiled201.Partition1.dynamic_expr(selected_binding, key, value) do
      nil -> nil
      result -> result
    end
  end
end
