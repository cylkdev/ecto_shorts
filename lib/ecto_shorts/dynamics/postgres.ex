defmodule EctoShorts.Adapters.Postgres do
  import Ecto.Query, only: [dynamic: 1]

  # alias EctoShorts.CommonSchema
  # alias EctoShorts.Adapters.Postgres.ArrayExpr
  # alias EctoShorts.Adapters.Postgres.ScalarExpr
  # alias EctoShorts.Adapters.Postgres.NamedBinding
  # alias EctoShorts.Adapters.Postgres.PositionalBinding

  def build_dynamic(source, binding_selector, {key, value}, opts \\ []) do
    value
    |> List.wrap()
    |> reduce_dynamic(source, binding_selector, opts)
  end

  defp reduce_dynamic(entries, source, binding_selector, opts) do
    Enum.reduce(entries, nil, fn {key, value}, dyn_acc ->
      expr = apply_field_expr(source, binding_selector, key, value, opts)
      merge_dynamic(dyn_acc, :and, expr)
    end)
  end

  defp apply_field_expr(source, binding_selector, key, term, _opts) do
    cond do
      field_type_of_array?(source, key) -> ArrayExpr.dynamic_expr(key, term)
      key in [:start_date] -> CommonExpr.dynamic_expr(key, term)
      true -> ScalarExpr.dynamic_expr(key, term)
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
