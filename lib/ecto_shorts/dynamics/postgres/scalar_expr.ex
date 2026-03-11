defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  @max_positional_bindings 10

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Comparison,
        directives: [:comparison],
        positions: @max_positional_bindings
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.Membership,
        directives: [:membership],
        positions: @max_positional_bindings
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.StringUpperLower,
        directives: [:string_transform],
        positions: @max_positional_bindings
      ],
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled.String,
        directives: [:string],
        positions: @max_positional_bindings
      ]
    ]

  @directives ScalarExprBuilder.directives()
  @comparison_directives ScalarExprBuilder.directives(:comparison)
  @equality_directives ScalarExprBuilder.directives(:equality)
  @string_directives ScalarExprBuilder.directives(:string)

  def directives, do: @directives

  def dynamic_expr(selected_binding, key, negated, term, _opts) do
    {op, term} = normalize_term(term)
    module = compiled_module_for(op, term)
    module.dynamic_expr(selected_binding, key, negated, {op, term})
  end

  defp normalize_term({_, _} = term), do: term
  defp normalize_term(value), do: {:==, value}

  defp compiled_module_for(:in, _) do
    __MODULE__.Compiled.Membership
  end

  defp compiled_module_for(op, value) when op in @equality_directives and is_list(value) do
    __MODULE__.Compiled.Membership
  end

  defp compiled_module_for(op, {transform, _})
       when op in @comparison_directives and transform in [:lower, :upper] do
    __MODULE__.Compiled.StringUpperLower
  end

  defp compiled_module_for(op, term) when op in @string_directives do
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
