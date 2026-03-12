defmodule EctoShorts.Adapters.Postgres do
  alias EctoShorts.CommonFilters.SetComparison
  alias EctoShorts.CommonFilters.FilterHelpers
  alias EctoShorts.CommonSchema

  alias EctoShorts.Dynamics.Postgres.{
    ArrayExpr,
    CommonExpr,
    ScalarExpr
  }

  @behaviour EctoShorts.DynamicExprBuilder

  @quantifier_directives [:all, :any]

  @impl true
  def build_dynamic(source, selected_binding, {key, term}, opts \\ []) when is_list(opts) do
    expr =
      term
      |> normalize_params()
      |> Enum.reduce(nil, fn entry, acc ->
        {merge_op, expr_entry} = expr_entry(key, entry)
        dyn = apply_expr(source, selected_binding, expr_entry, opts)
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
      normalize_keyword_params(term, [])
    else
      [term]
    end
  end

  defp normalize_params(term), do: [term]

  defp normalize_keyword_params([], acc), do: Enum.reverse(acc)

  defp normalize_keyword_params([head | tail], acc) do
    with acc2 <- normalize_keyword_params(head, acc) do
      normalize_keyword_params(tail, acc2)
    end
  end

  defp normalize_keyword_params({quantifier, payload}, acc)
       when quantifier in @quantifier_directives do
    [{quantifier, payload} | acc]
  end

  defp normalize_keyword_params({key, inner_term}, acc) do
    prepend_normalized_terms(key, normalize_params(inner_term), acc)
  end

  defp prepend_normalized_terms(_key, [], acc), do: acc

  defp prepend_normalized_terms(key, [normalized_term | rest], acc) do
    prepend_normalized_terms(key, rest, [{key, normalized_term} | acc])
  end

  defp expr_entry(key, {merge_op, term}) when merge_op in [:and, :or] do
    {merge_op, {key, term}}
  end

  defp expr_entry(key, term) do
    {:and, {key, term}}
  end

  defp build_expr(source, selected_binding, key, term, opts) do
    if binding_selector?(selected_binding) do
      {negated, term} = normalize_negation(term)
      term = normalize_quantified_term(key, term, opts)

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

  defp normalize_quantified_term(key, {quantifier, payload}, opts)
       when quantifier in @quantifier_directives do
    {:==, {quantifier, SetComparison.build_quantified_query(key, payload, opts)}}
  end

  defp normalize_quantified_term(_key, term, _opts), do: term

  defp array_field?(source, key) do
    case CommonSchema.get_schema_reflection(source, :type, key) do
      {:array, _} -> true
      _ -> false
    end
  end

  defp binding_selector?({:as, nil}), do: true
  defp binding_selector?({:as, name}) when is_atom(name), do: true
  defp binding_selector?({:at, position}) when is_integer(position) and position >= 1, do: true
  defp binding_selector?(_), do: false
end
