defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder

  @core_directives [:ids, :before, :after, :until, :since, :exists]

  @temporal_directives [:start_date, :end_date, :since_date, :until_date]

  use EctoShorts.Compiler,
    modules: [
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Core,
        directives: @core_directives,
        positions: 10
      ],
      [
        builder: CommonExprBuilder,
        module: __MODULE__.Compiled.Temporal,
        directives: @temporal_directives,
        positions: 10
      ]
    ]

  @directives CommonExprBuilder.directives()

  def directives, do: @directives

  def dynamic_expr(selected_binding, key, term, negated, _opts) do
    cond do
      key in @core_directives ->
        __MODULE__.Compiled.Core.dynamic_expr(selected_binding, key, term, negated)

      key in @temporal_directives ->
        __MODULE__.Compiled.Temporal.dynamic_expr(selected_binding, key, term, negated)

      true ->
        nil
    end
  end
end
