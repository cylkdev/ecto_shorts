defmodule EctoShorts.CompilerTest.FirstCompiled2438 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 1}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 2}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 3}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 4}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 5}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 6}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 7}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 8}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 9}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 10}, :id, negated, value) do
    {:first, value}
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end