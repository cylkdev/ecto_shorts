defmodule EctoShorts.Generator.ClauseSpec do
  @moduledoc since: "3.0.0"

  @callback operators :: list(atom())

  @callback specs_for(
              key :: atom(),
              selected_binding :: term(),
              body_ast :: Macro.t(),
              opts :: Keyword.t()
            ) :: term()

  @spec operators(module()) :: list(atom())
  def operators(builder), do: builder.operators()

  def specs_for(
        builder,
        operator,
        selected_binding,
        binding_body_asts,
        opts
      ) do
    builder.specs_for(
      operator,
      selected_binding,
      binding_body_asts,
      opts
    )
  end
end
