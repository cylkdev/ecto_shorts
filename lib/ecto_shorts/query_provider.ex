defmodule EctoShorts.QueryProvider do
  @moduledoc false

  alias EctoShorts.Config

  @default_adapter EctoShorts.CommonFilters.FragmentProvider

  @doc false
  def resolve_expression(binding_selector, expression_key, expression_params, opts \\ []) do
    query_source_provider =
      Keyword.get(opts, :query_source_provider, Config.query_source_provider()) ||
        @default_adapter

    unless Code.ensure_loaded?(query_source_provider) and
             function_exported?(query_source_provider, :resolve_expression, 3) do
      raise ArgumentError,
            "Expected expression resolver module to have a resolve_expression/3 function, got: #{inspect(query_source_provider)}"
    end

    query_source_provider.resolve_expression(binding_selector, expression_key, expression_params)
  end
end
