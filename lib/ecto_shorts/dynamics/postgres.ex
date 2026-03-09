defmodule EctoShorts.Adapters.Postgres do
  import Ecto.Query, only: [dynamic: 1]

  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics.Postgres.{ArrayExpr, CommonExpr, ScalarExpr}

  def build_dynamic(source, binding_selector, {key, term}, opts \\ []) do
    expr = apply_expr(source, binding_selector, {key, term}, opts)

    merge_dynamic(nil, :and, expr)
  end

  defp apply_expr(source, binding_selector, {key, term}, opts) do
    cond do
      is_map(term) and not is_struct(term) ->
        apply_expr(source, binding_selector, {key, Map.to_list(term)}, opts)

      Keyword.keyword?(term) ->
        Enum.reduce(term, nil, fn {inner_key, inner_value}, acc ->
          dyn = apply_expr(source, binding_selector, {key, {inner_key, inner_value}}, opts)
          merge_dynamic(acc, :and, dyn)
        end)

      true ->
        build_expr(source, binding_selector, key, term, opts)
    end
  end

  defp build_expr(source, binding_selector, key, term, opts) do
    if binding_selector?(binding_selector) do
      cond do
        key in CommonExpr.keys() ->
          CommonExpr.dynamic_expr(binding_selector, key, term, opts)

        field_type_of_array?(source, key) ->
          ArrayExpr.dynamic_expr(binding_selector, key, term, opts)

        true ->
          ScalarExpr.dynamic_expr(binding_selector, key, term, opts)
      end
    end
  end

  defp binding_selector?({:as, nil}), do: true
  defp binding_selector?({:as, binding_alias}) when is_atom(binding_alias), do: true
  defp binding_selector?({:at, position}) when is_integer(position) and position >= 1, do: true
  defp binding_selector?(_), do: false

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
