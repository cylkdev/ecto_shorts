defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder

  @core_operators [:ids, :before, :after, :until, :since, :exists]

  @temporal_operators [:start_date, :end_date, :since_date, :until_date]

  use EctoShorts.Compiler,
    modules: [
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Core,
        operators: @core_operators,
        positions: 10
      ],
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Temporal,
        operators: @temporal_operators,
        positions: 10
      ]
    ]

  @operators CommonExprBuilder.operators()

  def operators, do: @operators

  def dynamic_expr(selected_binding, operator, negated, term, _opts) do
    cond do
      operator in @core_operators ->
        __MODULE__.Compiled.Core.dynamic_expr(selected_binding, operator, negated, term)

      operator in @temporal_operators ->
        __MODULE__.Compiled.Temporal.dynamic_expr(selected_binding, operator, negated, term)

      true ->
        nil
    end
  end
end
