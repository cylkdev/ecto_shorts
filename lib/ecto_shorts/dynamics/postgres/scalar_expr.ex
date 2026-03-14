defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @max_positional_bindings 10
  @aggregate_helpers [:avg, :count, :max, :min, :sum]

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Comparison,
        operators: [:comparison],
        positions: @max_positional_bindings
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Membership,
        operators: [:membership],
        positions: @max_positional_bindings
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.StringUpperLower,
        operators: [:string_transform],
        positions: @max_positional_bindings
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.String,
        operators: [:string],
        positions: @max_positional_bindings
      ]
    ]

  @operators ScalarExprBuilder.operators()
  @comparison_operators ScalarExprBuilder.operators(:comparison)
  @equality_operators ScalarExprBuilder.operators(:equality)
  @string_operators ScalarExprBuilder.operators(:string)

  def operators, do: @operators

  def dynamic_expr(selected_binding, key, negated, term, _opts) do
    {op, term} = normalize_term(term)
    module = compiled_module_for(op, term)
    module.dynamic_expr(selected_binding, key, negated, {op, term})
  end

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

  defp compiled_module_for(:in, _) do
    __MODULE__.Compiled.Membership
  end

  defp compiled_module_for(op, value) when op in @equality_operators and is_list(value) do
    __MODULE__.Compiled.Membership
  end

  defp compiled_module_for(op, {transform, _})
       when op in @comparison_operators and transform in [:lower, :upper] do
    __MODULE__.Compiled.StringUpperLower
  end

  defp compiled_module_for(op, term) when op in @string_operators do
    case term do
      {transform, _} when transform in [:lower, :upper] ->
        __MODULE__.Compiled.StringUpperLower

      _ ->
        __MODULE__.Compiled.String
    end
  end

  defp compiled_module_for(_op, _term) do
    __MODULE__.Compiled.Comparison
  end
end
