# defmodule EctoShorts.CommonQueryAPI.Generator.ArrayExprBuilder do
#   @moduledoc """
#   Builds AST for Postgres array field operations used in query macros.
#   Each operator returns only the inner quoted expression (never a dynamic).
#   """

#   # Operator aliases for convenience
#   def build_expr(:eq, bindings, key, value), do: build_expr(:==, bindings, key, value)
#   def build_expr(:not, bindings, key, value), do: build_expr(:!=, bindings, key, value)

#   # Nil handling equality/inequality
#   def build_expr(:==, bindings, key, nil) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       is_nil(field(unquote(binding_var), unquote(key)))
#     end
#   end

#   def build_expr(:!=, bindings, key, nil) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       not is_nil(field(unquote(binding_var), unquote(key)))
#     end
#   end

#   # Case-insensitive equality helpers for arrays (LOWER/UPPER)
#   def build_expr(:==, bindings, key, {:lower, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         """
#         (
#           SELECT array_agg(LOWER(db_tag)) FROM unnest(?::text[]) AS db_tag
#         ) = (
#           SELECT array_agg(LOWER(input_tag)) FROM unnest(?::text[]) AS input_tag
#         )
#         """,
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   def build_expr(:==, bindings, key, {:upper, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         """
#         (
#           SELECT array_agg(UPPER(db_tag)) FROM unnest(?::text[]) AS db_tag
#         ) = (
#           SELECT array_agg(UPPER(input_tag)) FROM unnest(?::text[]) AS input_tag
#         )
#         """,
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   # Inequality counterparts
#   def build_expr(:!=, bindings, key, {:lower, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       not fragment(
#         """
#         (
#           SELECT array_agg(LOWER(db_tag)) FROM unnest(?::text[]) AS db_tag
#         ) = (
#           SELECT array_agg(LOWER(input_tag)) FROM unnest(?::text[]) AS input_tag
#         )
#         """,
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   def build_expr(:!=, bindings, key, {:upper, value}) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       not fragment(
#         """
#         (
#           SELECT array_agg(UPPER(db_tag)) FROM unnest(?::text[]) AS db_tag
#         ) = (
#           SELECT array_agg(UPPER(input_tag)) FROM unnest(?::text[]) AS input_tag
#         )
#         """,
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   # Equality
#   def build_expr(:==, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) == unquote(value)
#     end
#   end

#   # Inequality
#   def build_expr(:!=, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) != unquote(value)
#     end
#   end

#   # Less than
#   def build_expr(:<, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) < unquote(value)
#     end
#   end

#   # Less than or equal (list support)
#   def build_expr(:<=, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? <= ANY(?)", field(unquote(binding_var), unquote(key)), unquote(values))
#     end
#   end

#   # Less than or equal
#   def build_expr(:<=, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) <= unquote(value)
#     end
#   end

#   # Greater than
#   def build_expr(:>, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) > unquote(value)
#     end
#   end

#   # Greater than or equal (list support)
#   def build_expr(:>=, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? >= ANY(?)", field(unquote(binding_var), unquote(key)), unquote(values))
#     end
#   end

#   # Greater than or equal
#   def build_expr(:>=, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) >= unquote(value)
#     end
#   end

#   # Array contains (Postgres @> operator)
#   def build_expr(:contains, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? @> ?", field(unquote(binding_var), unquote(key)), unquote(value))
#     end
#   end

#   # Array is contained by (Postgres <@ operator)
#   def build_expr(:contained_by, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? <@ ?", field(unquote(binding_var), unquote(key)), unquote(value))
#     end
#   end

#   # Array overlaps (Postgres && operator)
#   def build_expr(:overlaps, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment("? && ?", field(unquote(binding_var), unquote(key)), unquote(value))
#     end
#   end

#   # IN operator (array membership)
#   def build_expr(:in, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       field(unquote(binding_var), unquote(key)) in unquote(value)
#     end
#   end

#   # LIKE/ILIKE/Regex for array elements (fragment-based)
#   def build_expr(:ilike, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         """
#         NOT EXISTS (
#           SELECT 1 FROM unnest(?) AS input_tag
#           WHERE NOT EXISTS (
#             SELECT 1 FROM unnest(?) AS db_tag
#             WHERE input_tag ILIKE db_tag
#           )
#         )
#         """,
#         unquote(values),
#         field(unquote(binding_var), unquote(key))
#       )
#     end
#   end

#   def build_expr(:ilike, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         "EXISTS (SELECT 1 FROM unnest(?) elem WHERE elem ILIKE ?)",
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   def build_expr(:like, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         """
#         NOT EXISTS (
#           SELECT 1 FROM unnest(?) AS input_tag
#           WHERE NOT EXISTS (
#             SELECT 1 FROM unnest(?) AS db_tag
#             WHERE input_tag LIKE db_tag
#           )
#         )
#         """,
#         unquote(values),
#         field(unquote(binding_var), unquote(key))
#       )
#     end
#   end

#   def build_expr(:like, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         "EXISTS (SELECT 1 FROM unnest(?) elem WHERE elem LIKE ?)",
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   def build_expr(:=~, bindings, key, values) when is_list(values) do
#     binding_var = extract_binding_var(bindings)

#     # Reduce into OR chain of ~* comparisons
#     inner =
#       Enum.reduce(values, nil, fn val, acc ->
#         expr =
#           quote do: fragment("? ~* ?", field(unquote(binding_var), unquote(key)), unquote(val))

#         if acc do
#           quote do: unquote(acc) or unquote(expr)
#         else
#           expr
#         end
#       end)

#     inner
#   end

#   def build_expr(:=~, bindings, key, value) do
#     binding_var = extract_binding_var(bindings)

#     quote do
#       fragment(
#         "EXISTS (SELECT 1 FROM unnest(?) elem WHERE elem ~* ?)",
#         field(unquote(binding_var), unquote(key)),
#         unquote(value)
#       )
#     end
#   end

#   # Private helpers
#   defp extract_binding_var([{_, binding_var}]), do: binding_var
#   defp extract_binding_var(bindings) when is_list(bindings), do: List.last(bindings)
# end
