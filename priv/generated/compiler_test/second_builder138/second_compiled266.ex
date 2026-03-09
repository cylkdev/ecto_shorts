defmodule EctoShorts.CompilerTest.SecondCompiled266 do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 1}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 2}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 3}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 4}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 5}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 6}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 7}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 8}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 9}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr({:at, 10}, :slug, value) do
    {:second, value}
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end