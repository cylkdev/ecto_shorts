defmodule EctoShorts.Dynamics.Adapter do
  @moduledoc false

  @callback operators() :: [atom()]

  @callback build_dynamic(
              source :: term(),
              binding_selector :: term(),
              key :: term(),
              expr :: term()
            ) :: term()
end
