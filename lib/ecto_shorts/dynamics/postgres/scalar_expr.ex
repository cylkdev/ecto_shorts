defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  alias EctoShorts.Dynamics.Postgres.ScalarExprBuilder

  use EctoShorts.Compiler,
    modules: [
      [
        builder: ScalarExprBuilder,
        module: __MODULE__.Compiled,
        positions: 10
      ]
    ]

  @keys ScalarExprBuilder.keys()

  def keys, do: @keys

  def dynamic_expr(selected_binding, key, term, _opts) do
    case term do
      {_op, _value} = term ->
        __MODULE__.Compiled.dynamic_expr(selected_binding, key, term)

      value ->
        __MODULE__.Compiled.dynamic_expr(selected_binding, key, {:==, value})
    end
  end
end
