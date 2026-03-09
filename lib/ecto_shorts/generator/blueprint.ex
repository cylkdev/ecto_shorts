defmodule EctoShorts.Generator.Blueprint do
  @type t() :: %__MODULE__{
          key: Macro.t(),
          head: Macro.t(),
          body: Macro.t(),
          guard: Macro.t() | nil
        }

  @enforce_keys [:key, :head, :body, :guard]
  defstruct @enforce_keys
end
