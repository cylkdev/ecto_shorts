defmodule EctoShorts.CompilerTest.SecondCompiled3075.Partition1 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 1}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 2}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 3}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 4}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 5}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 6}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 7}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 8}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 9}, :id, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 10}, :id, value) do
    {:second, value}
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end
