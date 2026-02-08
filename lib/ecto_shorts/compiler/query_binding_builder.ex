defmodule EctoShorts.Compiler.QueryBindingBuilder do
  @moduledoc false

  def query_binding_contracts(context, max_query_bindings) do
    binding_alias_var = Macro.var(:binding_alias, context)
    target_binding_var = Macro.var(:q, context)
    step_var = Macro.var(:_, context)

    positional_binding_patterns =
      build_binding_patterns(:positional, target_binding_var, step_var, max_query_bindings)

    named_binding_patterns =
      build_binding_patterns(
        :named,
        target_binding_var,
        binding_alias_var,
        max_query_bindings
      )

    all_binding_patterns = positional_binding_patterns ++ named_binding_patterns

    {target_binding_var, all_binding_patterns}
  end

  defp build_binding_patterns(:named, target_binding_var, binding_alias_var, _max_pos) do
    [
      {
        {:as, nil},
        [
          quote do
            unquote(target_binding_var)
          end
        ]
      },
      {
        {:as, binding_alias_var},
        [
          quote do
            {^unquote(binding_alias_var), unquote(target_binding_var)}
          end
        ]
      }
    ]
  end

  defp build_binding_patterns(:positional, target_binding_var, step_var, max_pos) do
    Enum.map(1..max_pos, fn i ->
      {{:at, i}, Enum.map(1..i, &if(&1 === i, do: target_binding_var, else: step_var))}
    end)
  end
end
