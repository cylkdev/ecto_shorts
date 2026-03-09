defmodule EctoShorts.Dynamics.Postgres.CommonExpr do
  @moduledoc false
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
        modes: :positional
      ]
    ]
end
