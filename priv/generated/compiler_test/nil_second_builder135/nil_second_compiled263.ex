defmodule EctoShorts.CompilerTest.NilSecondCompiled263 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :second_key, value) do
    {:second, value}
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end