# defmodule EctoShorts.CommonFilters.QueryProviders.NoOp do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Default no-op fragment expression provider.

#   Returns `{:error, :no_op}` for all expression keys.
#   Configure a custom provider via the `:query_provider` application
#   config or the `:query_provider` option to handle fragment-based
#   join sources and lock expressions.
#   """

#   @doc false
#   def build_fragment_expression(_selected_binding, _expression_key, _expression_params) do
#     {:error, :no_op}
#   end
# end
