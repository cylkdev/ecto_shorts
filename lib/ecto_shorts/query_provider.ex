defmodule EctoShorts.FragmentProvider do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Resolves expression callbacks via the configured fragment provider.

  Used internally by `EctoShorts.CommonFilters.Join` and
  `EctoShorts.CommonFilters.Filter` to resolve fragment-based join sources
  and lock expressions.

  ## Configuration

  By default this module delegates to `EctoShorts.CommonFilters.FragmentProviders.NoOp`.
  To override at the application level, set `:fragment_provider` in the
  EctoShorts application config:

      config :ecto_shorts,
        fragment_provider: MyApp.CustomFragments

  You can also override per-call via the `:fragment_provider` option passed
  to `EctoShorts.CommonFilters.convert_params_to_filter/3`.

  ## Implementing a custom provider

  A fragment provider module must export `build_fragment_expression/3`:

      defmodule MyApp.CustomFragments do
        def build_fragment_expression(binding_selector, expression_key, expression_params) do
          # return an Ecto fragment or query expression
        end
      end

  The module is validated at call time; a missing `build_fragment_expression/3`
  raises `ArgumentError`.

  See also `EctoShorts.Config.fragment_provider/0` and
  `EctoShorts.CommonFilters`.
  """

  alias EctoShorts.Config

  @default_adapter EctoShorts.CommonFilters.FragmentProviders.NoOp

  @doc false
  def build_fragment_expression(binding_selector, expression_key, expression_params, opts \\ []) do
    fragment_provider =
      Keyword.get(opts, :fragment_provider, Config.fragment_provider()) ||
        @default_adapter

    unless Code.ensure_loaded?(fragment_provider) and
             function_exported?(fragment_provider, :build_fragment_expression, 3) do
      raise ArgumentError,
            "Expected expression resolver module to have a build_fragment_expression/3 function, got: #{inspect(fragment_provider)}"
    end

    fragment_provider.build_fragment_expression(binding_selector, expression_key, expression_params)
  end
end
