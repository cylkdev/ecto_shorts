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

  @quantifier_operators [:all, :any]
  @arithmetic_value_operators [:+, :-, :*, :/]
  @datetime_wrappers [:datetime]
  @datetime_value_operators [:add, :ago, :from_now]

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

  defp normalize_params(term) do
    cond do
      is_map(term) and not is_struct(term) ->
        term
        |> Map.to_list()
        |> normalize_params()

      Keyword.keyword?(term) ->
        normalize_keyword_params(term, [])

      true ->
        [normalize_value_node(term)]
    end
  end

  defp normalize_keyword_params([], acc), do: Enum.reverse(acc)

  defp normalize_keyword_params([head | tail], acc) do
    with acc2 <- normalize_keyword_params(head, acc) do
      normalize_keyword_params(tail, acc2)
    end
  end

  defp normalize_keyword_params({quantifier, payload}, acc)
       when quantifier in @quantifier_operators do
    [{quantifier, payload} | acc]
  end

  defp normalize_keyword_params({op, inner_term}, acc) when op in @arithmetic_value_operators do
    [normalize_value_node({op, inner_term}) | acc]
  end

  defp normalize_keyword_params({wrapper, inner_term}, acc) when wrapper in @datetime_wrappers do
    [normalize_value_node({wrapper, inner_term}) | acc]
  end

  defp normalize_keyword_params({key, inner_term}, acc) do
    prepend_key(key, normalize_params(inner_term), acc)
  end

  defp prepend_key(_key, [], acc), do: acc

  defp prepend_key(key, [normalized_term | rest], acc) do
    prepend_key(key, rest, [{key, normalized_term} | acc])
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
    {:==, {quantifier, SetComparison.build_quantified_query(key, payload, opts)}}
  end

  defp normalize_quantified_term(_key, term, _opts), do: term

  defp normalize_value_node(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_value_node()
  end

  defp normalize_value_node({:field, field_name}) do
    {:field, normalize_field_name(field_name)}
  end

  defp normalize_value_node({:value, value}) do
    {:value, normalize_value_node(value)}
  end

  defp normalize_value_node({op, term}) when op in @arithmetic_value_operators do
    case term do
      [left, right] -> {op, {normalize_value_node(left), normalize_value_node(right)}}
      _ -> raise ArgumentError, "Expected ..., got: #{inspect(term)}"
    end
  end

  defp normalize_value_node({wrapper, term}) when wrapper in @datetime_wrappers do
    case normalize_datetime_wrapper_payload(term) do
      {datetime_op, datetime_term} when datetime_op in @datetime_value_operators ->
        {wrapper, {datetime_op, datetime_term}}

      _ ->
        raise ArgumentError, "Expected datetime wrapper payload, got: #{inspect(term)}"
    end
  end

  defp normalize_value_node({op, term}) when op in @datetime_value_operators do
    {op, normalize_datetime_node(term)}
  end

  defp normalize_value_node(field: field_name) do
    {:field, normalize_field_name(field_name)}
  end

  defp normalize_value_node(value: value) do
    {:value, normalize_value_node(value)}
  end

  defp normalize_value_node([]) do
    []
  end

  defp normalize_value_node([head | tail]) do
    [normalize_value_node(head) | normalize_value_node(tail)]
  end

  defp normalize_value_node(term), do: term

  defp normalize_datetime_wrapper_payload(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_datetime_wrapper_payload()
  end

  defp normalize_datetime_wrapper_payload(term) when is_list(term) do
    if Keyword.keyword?(term) do
      case term do
        [{datetime_op, datetime_term}] when datetime_op in @datetime_value_operators ->
          normalize_value_node({datetime_op, datetime_term})

        _ ->
          raise ArgumentError, "Expected datetime wrapper payload, got: #{inspect(term)}"
      end
    else
      raise ArgumentError, "Expected datetime wrapper payload, got: #{inspect(term)}"
    end
  end

  defp normalize_datetime_node(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_datetime_node()
  end

  defp normalize_datetime_node(term) when is_list(term) do
    if Keyword.keyword?(term) do
      field_name = Keyword.get(term, :field)
      count = Keyword.fetch!(term, :count)
      interval = Keyword.fetch!(term, :interval)

      []
      |> maybe_put_datetime_field(field_name)
      |> Kernel.++(count: count, interval: interval)
    else
      raise ArgumentError, "Expected datetime params to be a keyword payload, got: #{inspect(term)}"
    end
  end

  defp maybe_put_datetime_field(params, nil), do: params
  defp maybe_put_datetime_field(params, field_name), do: [{:field, normalize_field_name(field_name)} | params]

  defp normalize_field_name(field_name) when is_atom(field_name), do: field_name
  defp normalize_field_name(field_name) when is_binary(field_name), do: String.to_existing_atom(field_name)

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
