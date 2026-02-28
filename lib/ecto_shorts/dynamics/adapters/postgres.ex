defmodule EctoShorts.Dynamics.Adapters.Postgres do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Postgres-specific dynamic expression adapter.

  This api is split into the following components:

    * CommonExpr - Adds support for operators (`:ids`, `:before`, `:after`, `:start_date`, `:end_date`, `:exists`)
    * ArrayExpr - Adds support for array-aware field expressions
    * ScalarExpr - Adds support for general expressions
  """

  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics.Adapters.Postgres.{ArrayExpr, CommonExpr, ScalarExpr}

  @behaviour EctoShorts.Dynamics.Adapter

  @operators [:ids, :before, :after, :start_date, :end_date, :exists]

  @impl true
  def operators, do: @operators

  @impl true
  def build_dynamic(source, binding_selector, key, expr) do
    cond do
      key in @operators ->
        CommonExpr.apply_dynamic_expr(binding_selector, key, expr)

      match?({:array, _}, CommonSchema.get_schema_reflection(source, :type, key)) ->
        ArrayExpr.apply_dynamic_expr(binding_selector, key, expr)

      true ->
        ScalarExpr.apply_dynamic_expr(binding_selector, key, expr)
    end
  end
end
