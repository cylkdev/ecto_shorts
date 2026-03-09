defmodule EctoShorts.CompilerTest.SingleCompiled1097 do
  @moduledoc """
  This module provides compiled positional binding functions for the EctoShorts.CompilerTest.SingleBuilder1033 builder.
  """

  import Ecto.Query, only: [dynamic: 2]

  @type binding_selector :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc """
  Composes a dynamic query fragment based on the provided binding selector, key, and value.

  This function is used to build dynamic query fragments for Ecto queries based on the provided binding selector, key, and value.
  """
  def dynamic_expr({:as, binding_alias}, :id, value) do
    {:single, value}
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end