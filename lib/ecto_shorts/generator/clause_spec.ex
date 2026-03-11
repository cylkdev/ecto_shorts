defmodule EctoShorts.Generator.ClauseSpec do
  @moduledoc since: "3.0.0"

  @callback directives :: list(atom())

  @callback specs_for(
              key :: atom(),
              selected_binding :: term(),
              body_ast :: Macro.t(),
              opts :: Keyword.t()
            ) :: term()

  @spec directives(module()) :: list(atom())
  def directives(builder), do: builder.directives()

  def specs_for(
        builder,
        directive,
        selected_binding,
        binding_body_asts,
        opts
      ) do
    builder.specs_for(
      directive,
      selected_binding,
      binding_body_asts,
      opts
    )
  end
end
