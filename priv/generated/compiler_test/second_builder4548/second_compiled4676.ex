defmodule EctoShorts.CompilerTest.SecondCompiled4676 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 1}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 2}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 3}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 4}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 5}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 6}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 7}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 8}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 9}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 10}, :slug, negated, value) do
    {:second, value}
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end