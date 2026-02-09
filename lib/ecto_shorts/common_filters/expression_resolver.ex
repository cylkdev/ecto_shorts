defmodule EctoShorts.CommonFilters.ExpressionResolver do
  @moduledoc false

  alias EctoShorts.Config
  alias EctoShorts.CommonFilters.ExpressionResolver.Common

  @doc false
  def resolve_expression(binding_selector, expression_key, expression_params, opts \\ []) do
    expression_resolver =
      Keyword.get(opts, :expression_resolver, Config.expression_resolver()) || Common

    unless Code.ensure_loaded?(expression_resolver) and
             function_exported?(expression_resolver, :resolve_expression, 3) do
      raise ArgumentError,
            "Expected expression resolver module to have a resolve_expression/3 function, got: #{inspect(expression_resolver)}"
    end

    expression_resolver.resolve_expression(binding_selector, expression_key, expression_params)
  end
end
