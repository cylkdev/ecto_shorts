defmodule EctoShorts.Compiler do
  alias EctoShorts.Config
  alias EctoShorts.Dynamics.Helpers

  @default_max_positional_bindings 10

  @spec query_binding_contracts(atom(), keyword()) :: Macro.t()
  def query_binding_contracts(context \\ __MODULE__, opts \\ []) do
    max_positional_bindings = opts[:positions] || Config.max_positional_bindings() || @default_max_positional_bindings

    target_binding_var = Macro.var(:q, context)
    binding_alias_var = Macro.var(:binding_alias, context)
    step_var = Macro.var(:_, context)

    binding_patterns =
      [
        {{:as, nil}, [target_binding_var]},
        {{:as, binding_alias_var},
         [
           quote do
             {^unquote(binding_alias_var), unquote(target_binding_var)}
           end
         ]}
      ] ++
        Enum.map(1..max_positional_bindings, fn index ->
          {{:at, index}, Helpers.positional_binding_vars(index, target_binding_var, step_var)}
        end)

    {target_binding_var, binding_patterns}
  end
end
