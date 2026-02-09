defmodule EctoShorts.CommonFilters.FragmentProvider do
  @moduledoc false

  @doc false
  def resolve_expression(_binding_selector, _expression_key, _expression_params) do
    {:error, :unsupported_expression}
  end
end
