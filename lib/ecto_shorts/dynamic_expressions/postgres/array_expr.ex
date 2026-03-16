defmodule EctoShorts.DynamicExpressions.Postgres.ArrayExpr do
  alias Ecto.Query
  alias EctoShorts.QueryBinding

  require Ecto.Query

  {target_binding_var, binding_patterns} =
    QueryBinding.query_binding_contracts(__MODULE__)

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    def dynamic_expr(unquote(quoted_binding_head), key, negated, term, _opts) do
      expr =
        case normalize_term(term) do
          {:==, nil} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              is_nil(field(unquote(target_binding_var), ^key))
            )

          {:==, values} when is_list(values) ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), ^key) == ^values
            )

          {:!=, values} when is_list(values) ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), ^key) != ^values
            )

          {:==, {:lower, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "EXISTS (SELECT 1 FROM unnest(?) AS t WHERE lower(t) = ?)",
                field(unquote(target_binding_var), ^key),
                ^value
              )
            )

          {:==, {:upper, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "EXISTS (SELECT 1 FROM unnest(?) AS t WHERE upper(t) = ?)",
                field(unquote(target_binding_var), ^key),
                ^value
              )
            )

          {:!=, {:lower, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "NOT EXISTS (SELECT 1 FROM unnest(?) AS t WHERE lower(t) = ?)",
                field(unquote(target_binding_var), ^key),
                ^value
              )
            )

          {:!=, {:upper, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "NOT EXISTS (SELECT 1 FROM unnest(?) AS t WHERE upper(t) = ?)",
                field(unquote(target_binding_var), ^key),
                ^value
              )
            )

          {:==, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              ^value in field(unquote(target_binding_var), ^key)
            )

          {:!=, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              ^value not in field(unquote(target_binding_var), ^key)
            )

          {:in, values} when is_list(values) ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? && ?", field(unquote(target_binding_var), ^key), ^values)
            )

          {:in, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              ^value in field(unquote(target_binding_var), ^key)
            )

          {:count, {:>, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("array_length(?, 1)", field(unquote(target_binding_var), ^key)) > ^value
            )

          {:count, {:==, 0}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("coalesce(array_length(?, 1), 0)", field(unquote(target_binding_var), ^key)) == ^0
            )

          {:all, {:>, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? < ALL(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:all, {:>=, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? <= ALL(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:all, {:<, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? > ALL(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:all, {:<=, value}} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? >= ALL(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:all, {:in, values}} when is_list(values) ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? <@ ?", field(unquote(target_binding_var), ^key), ^values)
            )

          {:>, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? < ANY(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:>=, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? <= ANY(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:<, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? > ANY(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:<=, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment("? >= ANY(?)", ^value, field(unquote(target_binding_var), ^key))
            )

          {:lower, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "EXISTS (SELECT 1 FROM unnest(?) AS t WHERE lower(t) = ?)",
                field(unquote(target_binding_var), ^key),
                ^value
              )
            )

          {:upper, value} ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "EXISTS (SELECT 1 FROM unnest(?) AS t WHERE upper(t) = ?)",
                field(unquote(target_binding_var), ^key),
                ^value
              )
            )

          {:like, value} ->
            patterns = normalize_patterns(value)

            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "EXISTS (SELECT 1 FROM unnest(?) AS t WHERE t LIKE ANY (?))",
                field(unquote(target_binding_var), ^key),
                ^patterns
              )
            )

          {:ilike, value} ->
            patterns = normalize_patterns(value)

            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                "EXISTS (SELECT 1 FROM unnest(?) AS t WHERE t ILIKE ANY (?))",
                field(unquote(target_binding_var), ^key),
                ^patterns
              )
            )

          _ ->
            nil
        end

      if negated === :not and not is_nil(expr) do
        Query.dynamic([unquote_splicing(quoted_binding_body)], not (^expr))
      else
        expr
      end
    end
  end

  def dynamic_expr(_selected_binding, _key, _negated, _term, _opts), do: nil

  defp normalize_term({:all, payload}) do
    {:all, normalize_all_payload(payload)}
  end

  defp normalize_term({op, value}) do
    {normalize_operator(op), value}
  end

  defp normalize_term(nil) do
    {:==, nil}
  end

  defp normalize_term(value) when is_list(value) do
    {:==, value}
  end

  defp normalize_term(value) do
    {:in, value}
  end

  defp normalize_operator(:eq), do: :==
  defp normalize_operator(:ne), do: :!=
  defp normalize_operator(:gt), do: :>
  defp normalize_operator(:gte), do: :>=
  defp normalize_operator(:lt), do: :<
  defp normalize_operator(:lte), do: :<=
  defp normalize_operator(op), do: op

  defp normalize_all_payload(payload) when is_map(payload) and not is_struct(payload) do
    payload
    |> Map.to_list()
    |> normalize_all_payload()
  end

  defp normalize_all_payload(payload) when is_list(payload) do
    payload
    |> Enum.reduce([], &normalize_all_payload_entry/2)
    |> Enum.reverse()
    |> collapse_all_payload()
  end

  defp normalize_all_payload({op, value}) do
    {normalize_operator(op), value}
  end

  defp normalize_all_payload(payload), do: payload

  defp normalize_all_payload_entry({op, value}, payload) do
    [{normalize_operator(op), value} | payload]
  end

  defp normalize_all_payload_entry(value, payload) do
    [value | payload]
  end

  defp collapse_all_payload([payload]), do: payload
  defp collapse_all_payload(payload), do: payload

  defp normalize_patterns(values) when is_list(values) do
    Enum.map(values, &preserve_or_wrap_pattern/1)
  end

  defp normalize_patterns(value) do
    [preserve_or_wrap_pattern(value)]
  end

  defp preserve_or_wrap_pattern(value) when is_binary(value) do
    if String.contains?(value, ["%", "_"]) do
      value
    else
      "%#{value}%"
    end
  end

  defp preserve_or_wrap_pattern(value) do
    "%#{value}%"
  end
end
