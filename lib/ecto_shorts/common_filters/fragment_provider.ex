defmodule EctoShorts.CommonFilters.FragmentProvider do
  @moduledoc """
  Default no-op fragment expression provider.

  Returns `{:error, :unsupported_expression}` for all expression keys.
  Configure a custom provider via the `:query_source_provider` application
  config or the `:query_source_provider` option to handle fragment-based
  join sources and lock expressions.
  """

  @doc false
  def resolve_expression(_binding_selector, _expression_key, _expression_params) do
    {:error, :unsupported_expression}
  end
end
