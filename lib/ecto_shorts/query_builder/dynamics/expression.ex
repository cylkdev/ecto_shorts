defmodule EctoShorts.QueryBuilder.Dynamics.Expression do
  @moduledoc """
  Defines the spec-driven compiler for `dynamic_field_expr/3` clauses.

  This namespace provides:

  - A compile-time adapter hook (`ClauseAdapter`)

  Use this module as a convenience entrypoint when you want to build or validate
  clause specs without reaching into individual submodules.
  """

  @doc """
  Sets up the current module as an expression adapter.

  This delegates to `ClauseAdapter`.
  """
  defmacro __using__(_opts) do
    quote do
      use EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter
    end
  end
end
