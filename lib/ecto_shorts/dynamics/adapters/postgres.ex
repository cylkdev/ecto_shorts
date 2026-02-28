defmodule EctoShorts.Dynamics.Adapters.Postgres do
  @moduledoc """
  Postgres-specific dynamic expression adapter.

  Routes expression building to `CommonExpr`, `ArrayExpr`, or `ScalarExpr`
  based on the key and the schema field type. Handles special operators
  (`:ids`, `:before`, `:after`, `:start_date`, `:end_date`, `:exists`) via
  `CommonExpr`, array-typed fields via `ArrayExpr`, and everything else via
  `ScalarExpr`.
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
