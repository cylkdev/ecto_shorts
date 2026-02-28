defmodule EctoShorts.CommonFilters.FragmentProviders.NoOp do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Default no-op fragment expression provider.

  Returns `{:error, :no_op}` for all expression keys.
  Configure a custom provider via the `:fragment_provider` application
  config or the `:fragment_provider` option to handle fragment-based
  join sources and lock expressions.
  """

  @doc false
  def build_fragment_expression(_binding_selector, _expression_key, _expression_params) do
    {:error, :no_op}
  end
end
