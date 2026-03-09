defmodule EctoShorts.CompilerTest.BrokenCompiled454 do
  @moduledoc """
  This module provides compiled positional binding functions for the EctoShorts.CompilerTest.BrokenBuilder390 builder.
  """

  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  this(will(not compile))

  @doc """
  Composes a dynamic query fragment based on the provided binding selector, key, and value.

  This function is used to build dynamic query fragments for Ecto queries based on the provided binding selector, key, and value.
  """
  def dynamic_expr({:as, binding_alias}, :id, value) do
    {:broken, value}
  end

  def dynamic_expr({:at, 1}, :id, value) do
    {:broken, value}
  end

  def dynamic_expr({:at, 2}, :id, value) do
    {:broken, value}
  end

  def dynamic_expr({:at, 3}, :id, value) do
    {:broken, value}
  end

  def dynamic_expr({:at, 4}, :id, value) do
    {:broken, value}
  end

  def dynamic_expr({:at, 5}, :id, value) do
    {:broken, value}
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end