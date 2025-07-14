# defmodule EctoShorts.CommonQueryAPI.Generator.JoinExprBuilder do
#   def build_expr(:association, binding_info, {query_var, qual_var, key_var, value_var, opts_var} = _vars_info) do
#     binding_var = extract_binding_var(binding_info)

#     quote do
#       assoc(unquote(binding_var), unquote(key_var))
#     end
#   end

#   def build_expr(:subquery, _binding_info, {query_var, qual_var, key_var, value_var, opts_var} = _vars_info) do
#     quote do
#       subquery(unquote(value_var))
#     end
#   end

#   defp extract_binding_var({:positiinal, count, bindings}), do: Enum.at(bindings, count - 1)
#   defp extract_binding_var({:named, _binding_alias_var, binding_var}), do: binding_var
# end
