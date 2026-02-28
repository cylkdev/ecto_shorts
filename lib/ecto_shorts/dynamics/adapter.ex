defmodule EctoShorts.Dynamics.Adapter do
  @moduledoc """
  Defines the behaviour for dynamic expression adapters.

  A dynamic adapter must implement `operators/0` (returning the list of
  special operator atoms it handles) and `build_dynamic/4` (returning an
  `Ecto.Query.DynamicExpr` for a given source, binding, key, and expression).
  """

  @callback operators() :: [atom()]

  @callback build_dynamic(
              source :: term(),
              binding_selector :: term(),
              key :: term(),
              expr :: term()
            ) :: term()
end
