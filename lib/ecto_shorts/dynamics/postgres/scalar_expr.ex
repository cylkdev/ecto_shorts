defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  import Ecto.Query

  alias EctoShorts.Compiler
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @max_positional_bindings 10
  @aggregate_helpers [:avg, :count, :max, :min, :sum]

  @operators ScalarExprBuilder.operators()
  @comparison_operators ScalarExprBuilder.operators(:comparison)
  @equality_operators ScalarExprBuilder.operators(:equality)
  @string_operators ScalarExprBuilder.operators(:string)

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(@max_positional_bindings, __MODULE__)

  context = __MODULE__
  key_var = Macro.var(:key, context)
  negated_var = Macro.var(:negated, context)
  value_var = Macro.var(:value, context)

  def operators, do: @operators

  for {quoted_binding_head, _quoted_binding_body} <- binding_patterns do
    def dynamic_expr(selected_binding = unquote(quoted_binding_head), key, negated, term, _opts) do
      {op, normalized_term} = normalize_term(term)

      case family_for(op, normalized_term) do
        :membership ->
          membership_expr(selected_binding, key, negated, {op, normalized_term})

        :string_transform ->
          string_transform_expr(selected_binding, key, negated, {op, normalized_term})

        :string ->
          string_expr(selected_binding, key, negated, {op, normalized_term})

        :comparison ->
          comparison_expr(selected_binding, key, negated, {op, normalized_term})
      end
    end

    defp membership_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      unquote(
        ScalarExprBuilder.quote_body(
          :membership,
          quoted_binding_head,
          {target_binding_var, key_var, negated_var, value_var},
          context
        )
      )
    end

    defp comparison_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      unquote(
        ScalarExprBuilder.quote_body(
          :comparison,
          quoted_binding_head,
          {target_binding_var, key_var, negated_var, value_var},
          context
        )
      )
    end

    defp string_transform_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      unquote(
        ScalarExprBuilder.quote_body(
          :string_transform,
          quoted_binding_head,
          {target_binding_var, key_var, negated_var, value_var},
          context
        )
      )
    end

    defp string_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      unquote(
        ScalarExprBuilder.quote_body(
          :string,
          quoted_binding_head,
          {target_binding_var, key_var, negated_var, value_var},
          context
        )
      )
    end
  end

  def dynamic_expr(_selected_binding, _key, _negated, _term, _opts), do: nil

  defp normalize_term({transform, value}) when transform in [:lower, :upper] do
    {:==, {transform, value}}
  end

  defp normalize_term({op, value}) do
    normalized_op = normalize_operator(op)
    {normalized_op, normalize_term_value(normalized_op, value)}
  end

  defp normalize_term(value), do: {:==, value}

  defp normalize_operator(:eq), do: :==
  defp normalize_operator(:ne), do: :!=
  defp normalize_operator(:gt), do: :>
  defp normalize_operator(:gte), do: :>=
  defp normalize_operator(:lt), do: :<
  defp normalize_operator(:lte), do: :<=
  defp normalize_operator(op), do: op

  defp normalize_term_value(op, {nested_op, nested_value}) when op in @aggregate_helpers do
    {normalize_operator(nested_op), nested_value}
  end

  defp normalize_term_value(_op, value), do: value

  defp family_for(:in, _term), do: :membership

  defp family_for(op, value) when op in @equality_operators and is_list(value) do
    :membership
  end

  defp family_for(op, {transform, _term})
       when op in @comparison_operators and transform in [:lower, :upper] do
    :string_transform
  end

  defp family_for(op, term) when op in @string_operators do
    case term do
      {transform, _term} when transform in [:lower, :upper] ->
        :string_transform

      _ ->
        :string
    end
  end

  defp family_for(_op, _term), do: :comparison
end
