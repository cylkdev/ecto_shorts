defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr do
  @moduledoc since: "3.0.0"
  @moduledoc false

  use EctoShorts.Compiler,
    specs: [
      EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Aggregate,
      EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Arithmetic,
      EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.DateTime,
      EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Quantifier,
      EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Core
    ]
end
