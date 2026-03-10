defmodule EctoShorts.Adapters.Postgres do
  import Ecto.Query, only: [dynamic: 1]

  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics.Postgres.{ArrayExpr, CommonExpr, ScalarExpr}

  def build_dynamic(source, selected_binding, {key, term}, opts \\ []) do
    expr =
      term
      |> normalize_term()
      |> Enum.reduce(nil, fn entry, acc ->
        {merge_op, expr_entry} = expr_entry(key, entry)
        dyn = apply_expr(source, selected_binding, expr_entry, opts)

        merge_dynamic(acc, merge_op, dyn)
      end)

    merge_dynamic(nil, :and, expr)
  end

  defp apply_expr(source, selected_binding, {key, term}, opts) do
    cond do
      is_map(term) and not is_struct(term) ->
        apply_expr(source, selected_binding, {key, Map.to_list(term)}, opts)

      Keyword.keyword?(term) ->
        Enum.reduce(term, nil, fn {inner_key, inner_value}, acc ->
          dyn = apply_expr(source, selected_binding, {key, {inner_key, inner_value}}, opts)
          merge_dynamic(acc, :and, dyn)
        end)

      true ->
        build_expr(source, selected_binding, key, term, opts)
    end
  end

  defp normalize_term(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_term()
  end

  defp normalize_term(term) when is_list(term) do
    if Keyword.keyword?(term) do
      Enum.flat_map(term, fn {key, inner_term} ->
        normalize_term(inner_term)
        |> Enum.map(&{key, &1})
      end)
    else
      [term]
    end
  end

  defp normalize_term(term), do: [term]

  defp expr_entry(key, {merge_op, term}) when merge_op in [:and, :or] do
    {merge_op, {key, term}}
  end

  defp expr_entry(key, term) do
    {:and, {key, term}}
  end

  defp build_expr(source, selected_binding, key, term, opts) do
    if binding_selector?(selected_binding) do
      {negated, term} = normalize_negation(term)

      cond do
        key in CommonExpr.keys() ->
          CommonExpr.dynamic_expr(selected_binding, key, negated, term, opts)

        field_type_of_array?(source, key) ->
          ArrayExpr.dynamic_expr(selected_binding, key, negated, term, opts)

        true ->
          ScalarExpr.dynamic_expr(selected_binding, key, negated, term, opts)
      end
    end
  end

  defp normalize_negation({:not, term}), do: {:not, term}
  defp normalize_negation(term), do: {nil, term}

  defp binding_selector?({:as, nil}), do: true
  defp binding_selector?({:as, name}) when is_atom(name), do: true
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
