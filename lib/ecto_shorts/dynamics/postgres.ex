defmodule EctoShorts.Adapters.Postgres do
  import Ecto.Query, only: [dynamic: 1]

  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics.Postgres.ArrayExpr
  alias EctoShorts.Dynamics.Postgres.CommonExpr
  alias EctoShorts.Dynamics.Postgres.ScalarExpr

  def build_dynamic(source, binding_selector, {key, value}, opts \\ []) do
    expr = apply_field_expr(source, binding_selector, key, value, opts)
    merge_dynamic(nil, :and, expr)
  end

  defp apply_field_expr(source, binding_selector, key, value, opts) do
    cond do
      field_type_of_array?(source, key) -> ArrayExpr.dynamic_expr(binding_selector, key, value, opts)
      key in CommonExpr.keys() -> CommonExpr.dynamic_expr(binding_selector, key, value, opts)
      true -> ScalarExpr.dynamic_expr(binding_selector, key, value, opts)
    end
  end

  defp field_type_of_array?(source, key) do
    case CommonSchema.get_schema_reflection(source, :type, key) do
      {:array, _} -> true
      _ -> false
    end
  end

  defp merge_dynamic(nil, _, b), do: b
  defp merge_dynamic(a, _, nil), do: a
  defp merge_dynamic(a, :and, b), do: dynamic(^a and ^b)
  defp merge_dynamic(a, :or, b), do: dynamic(^a or ^b)
end
