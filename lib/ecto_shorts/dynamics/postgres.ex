defmodule EctoShorts.Adapters.Postgres do
  alias EctoShorts.CommonFilters.FilterHelpers
  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics.Postgres.{ArrayExpr, CommonExpr, ScalarExpr}

  def build_dynamic(source, selected_binding, {key, term}, opts \\ []) do
    expr =
      term
      |> normalize_params()
      |> Enum.reduce(nil, fn entry, acc ->
        {merge_op, normalize_expr_entry} = normalize_expr_entry(key, entry)
        dyn = apply_expr(source, selected_binding, normalize_expr_entry, opts)
        FilterHelpers.merge_dynamic(acc, merge_op, dyn)
      end)

    FilterHelpers.merge_dynamic(nil, :and, expr)
  end

  defp apply_expr(source, selected_binding, {key, term}, opts) do
    cond do
      is_map(term) and not is_struct(term) ->
        apply_expr(source, selected_binding, {key, Map.to_list(term)}, opts)

      Keyword.keyword?(term) ->
        Enum.reduce(term, nil, fn {inner_key, inner_value}, acc ->
          dyn = apply_expr(source, selected_binding, {key, {inner_key, inner_value}}, opts)
          FilterHelpers.merge_dynamic(acc, :and, dyn)
        end)

      true ->
        build_expr(source, selected_binding, key, term, opts)
    end
  end

  defp normalize_params(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_params()
  end

  defp normalize_params(term) when is_list(term) do
    if Keyword.keyword?(term) do
      Enum.flat_map(term, fn {key, inner_term} ->
        inner_term
        |> normalize_params()
        |> Enum.map(&{key, &1})
      end)
    else
      [term]
    end
  end

  defp normalize_params(term), do: [term]

  defp normalize_expr_entry(key, {merge_op, term}) when merge_op in [:and, :or] do
    {merge_op, {key, term}}
  end

  defp normalize_expr_entry(key, term) do
    {:and, {key, term}}
  end

  defp build_expr(source, selected_binding, key, term, opts) do
    if FilterHelpers.binding_selector?(selected_binding) do
      {negated, term} = normalize_negation(term)

      cond do
        key in CommonExpr.directives() ->
          CommonExpr.dynamic_expr(selected_binding, key, negated, term, opts)

        array_field?(source, key) ->
          ArrayExpr.dynamic_expr(selected_binding, key, negated, term, opts)

        true ->
          ScalarExpr.dynamic_expr(selected_binding, key, negated, term, opts)
      end
    end
  end

  defp normalize_negation({:not, term}), do: {:not, term}
  defp normalize_negation(term), do: {nil, term}

  defp array_field?(source, key) do
    case CommonSchema.get_schema_reflection(source, :type, key) do
      {:array, _} -> true
      _ -> false
    end
  end
end
