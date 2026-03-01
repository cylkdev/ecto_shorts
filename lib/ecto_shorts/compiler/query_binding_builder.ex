defmodule EctoShorts.Compiler.QueryBindingBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Generates positional and named binding pattern ASTs at compile time.

  Produces a list of `{binding_head, binding_body}` tuples for every
  positional index from 1 to `max_binding_positions`, plus two named binding
  patterns (`{:as, nil}` and `{:as, alias}`). These patterns are used by
  `EctoShorts.Compiler` and `Compiler.define_clauses/2` to generate
  multi-clause functions that match on binding selectors.
  """

  def query_binding_contracts(context, max_binding_positions) do
    binding_alias_var = Macro.var(:binding_alias, context)
    target_binding_var = Macro.var(:q, context)
    step_var = Macro.var(:_, context)

    positional_binding_patterns =
      build_binding_patterns(:positional, target_binding_var, step_var, max_binding_positions)

    named_binding_patterns =
      build_binding_patterns(
        :named,
        target_binding_var,
        binding_alias_var,
        max_binding_positions
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
