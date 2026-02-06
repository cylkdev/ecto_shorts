defmodule EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr do
  @moduledoc false

  use EctoShorts.Dynamics.Compiler,
    specs: EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs,
    max_positional_bindings: 10
end
