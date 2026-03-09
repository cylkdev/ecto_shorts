defmodule EctoShorts.Generator.ClauseSpec do
  @moduledoc since: "3.0.0"

  @callback keys :: list(atom())

  @callback specs_for(
              key :: atom(),
              binding_selector :: term(),
              target_binding_var :: Macro.t(),
              opts :: Keyword.t()
            ) :: term()

  @spec keys(module()) :: list(atom())
  def keys(builder), do: builder.keys()

  def specs_for(
        builder,
        key,
        binding_selector,
        binding_body_asts,
        opts
      ) do
    builder.specs_for(
      key,
      binding_selector,
      binding_body_asts,
      opts
    )
  end
end
