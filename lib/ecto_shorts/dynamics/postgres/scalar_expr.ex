defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @max_binding_positions 10

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Comparison,
        keys: [:comparison],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Membership,
        keys: [:membership],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.StringUpperLower,
        keys: [:string_upper_lower],
        positions: @max_binding_positions
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.String,
        keys: [:string],
        positions: @max_binding_positions
      ]
    ]

  @keys ScalarExprBuilder.keys()

  def keys, do: @keys

  def dynamic_expr(selected_binding, key, term, negated, _opts) do
    routed_term = normalize_scalar_term(term)

    {module, routed_term} = dispatch(routed_term)

    module.dynamic_expr(selected_binding, key, routed_term, negated)
  end

  defp normalize_scalar_term({_op, _value} = tuple_term), do: tuple_term
  defp normalize_scalar_term(value), do: {:==, value}

  defp dispatch({:in, _value} = term) do
    {__MODULE__.Compiled.Membership, term}
  end

  defp dispatch({op, value} = term) when op in [:==, :eq, :!=, :ne] and is_list(value) do
    {__MODULE__.Compiled.Membership, term}
  end

  defp dispatch({op, term}) when op in [:like, :ilike] do
    case term do
      {transform, value} when transform in [:lower, :upper] ->
        {__MODULE__.Compiled.StringUpperLower, value}

      value ->
        {__MODULE__.Compiled.String, value}
    end
  end

  defp dispatch(term) do
    {__MODULE__.Compiled.Comparison, term}
  end
end
