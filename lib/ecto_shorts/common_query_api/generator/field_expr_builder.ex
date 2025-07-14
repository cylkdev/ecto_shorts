# defmodule EctoShorts.CommonQueryAPI.Generator.FieldExprBuilder do
#   @moduledoc """
#   Builds AST for scalar (non-array) field operations used in query macros.
#   Each operator returns only the inner quoted expression (never a dynamic).
#   """

#   # Equality and inequality operators

#   def build_expr(:eq, binding_info, {query_var, qual_var, key_var, value_var, opts_var} = _vars_info) do
#     build_expr(:==, binding_info, {query_var, qual_var, key_var, value_var, opts_var})
#   end

#   def build_expr(:not, binding_info, {query_var, qual_var, key_var, value_var, opts_var} = _vars_info) do
#     build_expr(:!=, binding_info, {query_var, qual_var, key_var, value_var, opts_var})
#   end

#   # ==
#   def build_expr(:==, binding_info, {query_var, qual_var, key_var, value_var, opts_var} = _vars_info) do
#     binding_var = extract_binding_var(binding_info)

#     quote do
#       is_nil(field(unquote(binding_var), ^unquote(key_var)))
#     end
#   end

#   def build_expr(:==, binding_info, {query_var, qual_var, key_var, value_var, opts_var} = _vars_info) when is_list(values) do
#     binding_var = extract_binding_var(binding_info)

#     quote do
#       field(unquote(binding_var), ^unquote(key_var)) in ^unquote(values)
#     end
#   end

#   def build_expr(:==, bindings, key, {:lower, values}) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("LOWER(?)", field(unquote(binding_var), ^unquote(key))) in ^unquote(values)
#     end
#   end

#   def build_expr(:==, bindings, key, {:lower, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("LOWER(?)", field(unquote(binding_var), ^unquote(key))) == ^unquote(value)
#     end
#   end

#   def build_expr(:==, bindings, key, {:upper, values}) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("UPPER(?)", field(unquote(binding_var), ^unquote(key))) in ^unquote(values)
#     end
#   end

#   def build_expr(:==, bindings, key, {:upper, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("UPPER(?)", field(unquote(binding_var), ^unquote(key))) == ^unquote(value)
#     end
#   end

#   def build_expr(:==, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) == ^unquote(value)
#     end
#   end

#   # !=
#   def build_expr(:!=, bindings, key, nil) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       not is_nil(field(unquote(binding_var), ^unquote(key)))
#     end
#   end

#   def build_expr(:!=, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) not in ^unquote(values)
#     end
#   end

#   def build_expr(:!=, bindings, key, {:lower, values}) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("LOWER(?)", field(unquote(binding_var), ^unquote(key))) not in ^unquote(values)
#     end
#   end

#   def build_expr(:!=, bindings, key, {:lower, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("LOWER(?)", field(unquote(binding_var), ^unquote(key))) != ^unquote(value)
#     end
#   end

#   def build_expr(:!=, bindings, key, {:upper, values}) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("UPPER(?)", field(unquote(binding_var), ^unquote(key))) not in ^unquote(values)
#     end
#   end

#   def build_expr(:!=, bindings, key, {:upper, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("UPPER(?)", field(unquote(binding_var), ^unquote(key))) != ^unquote(value)
#     end
#   end

#   def build_expr(:!=, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) != ^unquote(value)
#     end
#   end

#   # Comparison operators

#   def build_expr(:<, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? < ANY(?)", field(unquote(binding_var), ^unquote(key)), ^unquote(values))
#     end
#   end

#   def build_expr(:<, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) < ^unquote(value)
#     end
#   end

#   def build_expr(:<=, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? <= ANY(?)", field(unquote(binding_var), ^unquote(key)), ^unquote(values))
#     end
#   end

#   def build_expr(:<=, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) <= ^unquote(value)
#     end
#   end

#   def build_expr(:>, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? > ANY(?)", field(unquote(binding_var), ^unquote(key)), ^unquote(values))
#     end
#   end

#   def build_expr(:>, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) > ^unquote(value)
#     end
#   end

#   # Comparison operators
#   def build_expr(:>=, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? >= ANY(?)", field(unquote(binding_var), ^unquote(key)), ^unquote(values))
#     end
#   end

#   def build_expr(:>=, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) >= ^unquote(value)
#     end
#   end

#   # :in operator (for scalar fields)
#   def build_expr(:in, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), ^unquote(key)) in ^unquote(value)
#     end
#   end

#   # :fragment_like operator (stub, if needed by legacy API)
#   def build_expr(:fragment_like, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? ILIKE ?", field(unquote(binding_var), ^unquote(key)), ^unquote(value))
#     end
#   end

#   # Pattern matching / string operators

#   def build_expr(:ilike, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         "? ILIKE ANY(SELECT unnest(?))",
#         field(unquote(binding_var), ^unquote(key)),
#         ^unquote(values)
#       )
#     end
#   end

#   def build_expr(:ilike, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       ilike(field(unquote(binding_var), ^unquote(key)), ^unquote(value))
#     end
#   end

#   def build_expr(:like, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         "? LIKE ANY(SELECT unnest(?))",
#         field(unquote(binding_var), ^unquote(key)),
#         ^unquote(values)
#       )
#     end
#   end

#   def build_expr(:like, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       like(field(unquote(binding_var), ^unquote(key)), ^unquote(value))
#     end
#   end

#   def build_expr(:=~, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? ~* ANY(?)", field(unquote(binding_var), ^unquote(key)), ^unquote(values))
#     end
#   end

#   def build_expr(:=~, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? ~* ?", field(unquote(binding_var), ^unquote(key)), ^unquote(value))
#     end
#   end

#   defp extract_binding_var([{_, binding_var}]), do: binding_var
#   defp extract_binding_var(bindings) when is_list(bindings), do: List.last(bindings)
# end
