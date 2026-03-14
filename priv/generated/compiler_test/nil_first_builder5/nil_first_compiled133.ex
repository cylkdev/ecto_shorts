defmodule EctoShorts.CompilerTest.NilFirstCompiled133 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 1}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 2}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 3}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 4}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 5}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 6}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 7}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 8}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 9}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr({:at, 10}, :first_key, negated, value) do
    {:first, value}
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end