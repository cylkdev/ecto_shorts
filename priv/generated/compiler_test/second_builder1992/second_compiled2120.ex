defmodule EctoShorts.CompilerTest.SecondCompiled2120 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :slug, _negated, value) do
    _selected_binding = {:as, binding_alias}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 1}, :slug, _negated, value) do
    _selected_binding = {:at, 1}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 2}, :slug, _negated, value) do
    _selected_binding = {:at, 2}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 3}, :slug, _negated, value) do
    _selected_binding = {:at, 3}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 4}, :slug, _negated, value) do
    _selected_binding = {:at, 4}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 5}, :slug, _negated, value) do
    _selected_binding = {:at, 5}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 6}, :slug, _negated, value) do
    _selected_binding = {:at, 6}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 7}, :slug, _negated, value) do
    _selected_binding = {:at, 7}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 8}, :slug, _negated, value) do
    _selected_binding = {:at, 8}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 9}, :slug, _negated, value) do
    _selected_binding = {:at, 9}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr({:at, 10}, :slug, _negated, value) do
    _selected_binding = {:at, 10}
    dynamic([], true)
    {:second, value}
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end