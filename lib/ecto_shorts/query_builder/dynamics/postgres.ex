defmodule EctoShorts.QueryBuilder.Dynamics.Postgres do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.QueryBuilder.Dynamics.Postgres.{ArrayExpr, CommonExpr, ScalarExpr}

  @operators [:ids, :before, :after, :start_date, :end_date]

  def operators do
    @operators
  end

  def build_dynamic_expression(source, binding_selector, key, expr) do
    cond do
      key in operators() ->
        CommonExpr.dynamic_field_expr(binding_selector, key, expr)

      match?({:array, _}, CommonSchema.get_schema_reflection(source, :type, key)) ->
        ArrayExpr.dynamic_field_expr(binding_selector, key, expr)

      true ->
        ScalarExpr.dynamic_field_expr(binding_selector, key, expr)
    end
  end
end
