defmodule EctoShorts.Compiler.QueryBindingBuilder do
  alias EctoShorts.Dynamics.Helpers

  def query_binding_contracts(context, max_binding_positions) do
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
        Enum.map(1..max_binding_positions, fn index ->
          {{:at, index}, Helpers.positional_binding_vars(index, target_binding_var, step_var)}
        end)

    {target_binding_var, binding_patterns}
  end
end
