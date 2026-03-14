defmodule EctoShorts.CompilerTest.PartitionedCompiledSecond452 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :second_key, _negated, value) do
    _selected_binding = {:as, binding_alias}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 1}, :second_key, _negated, value) do
    _selected_binding = {:at, 1}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 2}, :second_key, _negated, value) do
    _selected_binding = {:at, 2}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 3}, :second_key, _negated, value) do
    _selected_binding = {:at, 3}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 4}, :second_key, _negated, value) do
    _selected_binding = {:at, 4}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 5}, :second_key, _negated, value) do
    _selected_binding = {:at, 5}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 6}, :second_key, _negated, value) do
    _selected_binding = {:at, 6}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 7}, :second_key, _negated, value) do
    _selected_binding = {:at, 7}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 8}, :second_key, _negated, value) do
    _selected_binding = {:at, 8}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 9}, :second_key, _negated, value) do
    _selected_binding = {:at, 9}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr({:at, 10}, :second_key, _negated, value) do
    _selected_binding = {:at, 10}
    dynamic([], true)
    {:second_key, value}
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end