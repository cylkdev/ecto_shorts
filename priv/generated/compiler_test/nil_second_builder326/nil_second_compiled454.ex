defmodule EctoShorts.CompilerTest.NilSecondCompiled454 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 1}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 2}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 3}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 4}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 5}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 6}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 7}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 8}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 9}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 10}, :second_key, negated, value) do
    {:second, value}
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end