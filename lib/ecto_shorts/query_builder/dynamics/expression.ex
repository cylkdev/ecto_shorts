defmodule EctoShorts.QueryBuilder.Dynamics.Expression do
  @moduledoc """
  Defines the spec-driven compiler for `dynamic_field_expr/3` clauses.

  This namespace provides:

  - A clause spec struct (`EctoShorts.QueryBuilder.Dynamics.Expression.ClauseSpec`)
  - A clause compiler (`EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilder`)
  - A compile-time adapter hook (`EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter`)

  Use this module as a convenience entrypoint when you want to build or validate
  clause specs without reaching into individual submodules.
  """

  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilder
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseSpec

  @doc """
  Sets up the current module as an expression adapter.

  This delegates to `EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter`.
  """
  defmacro __using__(_opts) do
    quote do
      use EctoShorts.QueryBuilder.Dynamics.Expression.ClauseAdapter
    end
  end

  @doc """
  Builds a quoted `dynamic_field_expr/3` clause using the given emitter module.
  """
  @spec clause_ast(module(), ClauseSpec.t() | map() | keyword()) ::
          {:ok, Macro.t()} | {:error, term()}
  def clause_ast(emitter, spec) do
    ClauseBuilder.clause_ast(emitter, spec)
  end
end
