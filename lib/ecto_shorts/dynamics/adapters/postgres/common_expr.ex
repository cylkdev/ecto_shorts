defmodule EctoShorts.Dynamics.Adapters.Postgres.CommonExpr do
  @moduledoc false

  use EctoShorts.Compiler,
    specs: EctoShorts.Dynamics.Adapters.Postgres.CommonExpr.Specs,
    max_positional_bindings: 10
end
