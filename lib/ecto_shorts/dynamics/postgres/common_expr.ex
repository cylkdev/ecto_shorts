defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  @moduledoc """

  Example with a named binding:

      EctoShorts.Dynamics.Postgres.CommonExpr.dynamic_expr(
        EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.NamedBinding,
        {:as, nil},
        :start_date,
        ~U[2026-01-01 00:00:00Z]
      )

  Example with a positional binding:

      EctoShorts.Dynamics.Postgres.CommonExpr.dynamic_expr(
        EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.PositionalBinding,
        {:at, 1},
        :ids,
        [1, 2, 3]
      )

  Example with negation:

      EctoShorts.Dynamics.Postgres.CommonExpr.dynamic_expr(
        EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.NamedBinding,
        {:as, nil},
        :before,
        {:not, 10}
      )
  """
  use EctoShorts.Compiler,
    modules: [
      [
        builder: EctoShorts.Dynamics.Postgres.CommonExpr.Spec,
        module: EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.NamedBinding,
        modes: :named
      ],
      [
        builder: EctoShorts.Dynamics.Postgres.CommonExpr.Spec,
        module: EctoShorts.Dynamics.Postgres.CommonExpr.Compiled.PositionalBinding,
        modes: :positional,
        positions: 10,
        partitions: 2
      ]
    ]
end
