defmodule EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr do
  @moduledoc since: "3.0.0"
  @moduledoc false
  use EctoShorts.Compiler,
    specs: [
      EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.Core,
      EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.Aggregate
    ]
end
