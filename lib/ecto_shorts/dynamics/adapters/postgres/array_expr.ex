defmodule EctoShorts.Dynamics.Postgres.ArrayExpr do
  @moduledoc since: "3.0.0"
  @moduledoc false
  use EctoShorts.Compiler,
    specs: [
      EctoShorts.Dynamics.Postgres.ArrayExpr.Specs.LowerUpper,
      EctoShorts.Dynamics.Postgres.ArrayExpr.Specs.LikeIlike,
      EctoShorts.Dynamics.Postgres.ArrayExpr.Specs.Core,
      EctoShorts.Dynamics.Postgres.ArrayExpr.Specs.Aggregate
    ]
end
