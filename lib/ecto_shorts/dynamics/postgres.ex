defmodule EctoShorts.Dynamics.Postgres do
  alias EctoShorts.{
    CommonSchema,
    CommonFilters.FilterHelpers,
    CommonFilters.SetComparison,
    Dynamics.Postgres.ArrayExpr,
    Dynamics.Postgres.CommonExpr,
    Dynamics.Postgres.Normalizer,
    Dynamics.Postgres.ScalarExpr
  }

  @behaviour EctoShorts.Adapter.Dynamic

  @quantifier_operators [:all, :any]

  @impl true
  def build_dynamic(source, selected_binding, args, opts \\ [])

  def build_dynamic(source, selected_binding, {quantifier_op, params}, opts)
      when quantifier_op in @quantifier_operators do
    expr =
      params
      |> then(&Normalizer.normalize_params(source, &1, opts))
      |> Enum.reduce(nil, fn entry, acc ->
        dyn = apply_expr(source, selected_binding, entry, opts)
        FilterHelpers.merge_dynamic(acc, quantifier_op, dyn)
      end)

    FilterHelpers.merge_dynamic(nil, :and, expr)
  end

  def build_dynamic(source, selected_binding, {key, params}, opts) do
    expr =
      params
      |> then(&Normalizer.normalize_params(source, &1, opts))
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

  defp expr_entry(key, {merge_op, term}) when merge_op in [:and, :or] do
    {merge_op, {key, term}}
  end

  defp expr_entry(key, term) do
    {:and, {key, term}}
  end

  defp build_expr(source, selected_binding, key, term, opts) do
    if binding_selector?(selected_binding) do
      {negated, term} = normalize_negation_term(term)

      term = normalize_quantified_term(key, term, opts)

      cond do
        key in CommonExpr.operators() ->
          CommonExpr.dynamic_expr(selected_binding, key, negated, term, opts)

        array_field?(source, key) ->
          ArrayExpr.dynamic_expr(selected_binding, key, negated, term, opts)

        true ->
          ScalarExpr.dynamic_expr(
            selected_binding,
            key,
            negated,
            term,
            opts
          )
      end
    end
  end

  defp normalize_negation_term({:not, term}), do: {:not, term}
  defp normalize_negation_term(term), do: {nil, term}

  defp normalize_quantified_term(key, {quantifier, payload}, opts)
       when quantifier in @quantifier_operators do
    if quantified_query_payload?(payload) do
      {:==, {quantifier, SetComparison.build_quantified_query(key, payload, opts)}}
    else
      {quantifier, payload}
    end
  end

  defp normalize_quantified_term(key, {op, {quantifier, payload}}, opts)
       when quantifier in @quantifier_operators do
    if quantified_query_payload?(payload) do
      {op, {quantifier, SetComparison.build_quantified_query(key, payload, opts)}}
    else
      {op, {quantifier, payload}}
    end
  end

  defp normalize_quantified_term(_key, term, _opts), do: term

  defp quantified_query_payload?(payload) when is_map(payload) and not is_struct(payload) do
    Map.has_key?(payload, :from)
  end

  defp quantified_query_payload?(payload) when is_list(payload) do
    Keyword.keyword?(payload) and Keyword.has_key?(payload, :from)
  end

  defp quantified_query_payload?(_payload), do: false

  defp array_field?(source, key) do
    case CommonSchema.get_schema_reflection(source, :type, key) do
      {:array, _} -> true
      {:map, _} -> true
      _ -> false
    end
  end

  defp binding_selector?({:as, nil}), do: true
  defp binding_selector?({:as, name}) when is_atom(name), do: true
  defp binding_selector?({:at, position}) when is_integer(position) and position >= 1, do: true
  defp binding_selector?(_), do: false
end
