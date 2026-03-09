defmodule EctoShorts.CompilerTest.PartitionedCompiled74.Partition2 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:at, 5}, :second_key, value) do
    {:second_key, value}
  end

  def dynamic_expr({:at, 6}, :first_key, value) do
    {:first_key, value}
  end

  def dynamic_expr({:at, 6}, :second_key, value) do
    {:second_key, value}
  end

  def dynamic_expr({:at, 7}, :first_key, value) do
    {:first_key, value}
  end

  def dynamic_expr({:at, 7}, :second_key, value) do
    {:second_key, value}
  end

  def dynamic_expr({:at, 8}, :first_key, value) do
    {:first_key, value}
  end

  def dynamic_expr({:at, 8}, :second_key, value) do
    {:second_key, value}
  end

  def dynamic_expr({:at, 9}, :first_key, value) do
    {:first_key, value}
  end

  def dynamic_expr({:at, 9}, :second_key, value) do
    {:second_key, value}
  end

  def dynamic_expr({:at, 10}, :first_key, value) do
    {:first_key, value}
  end

  def dynamic_expr({:at, 10}, :second_key, value) do
    {:second_key, value}
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end
