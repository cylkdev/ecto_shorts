defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  @moduledoc since: "3.0.0"
  @moduledoc false

  use EctoShorts.Compiler,
    specs: [
      EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.Aggregate,
      EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.Arithmetic,
      EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.DateTime,
      EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.Quantifier,
      EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.Core
    ]
end
