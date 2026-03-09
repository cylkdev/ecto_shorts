# defmodule EctoShorts.Dynamics.Postgres.ArrayExpr.Specs.LowerUpper do
#   @moduledoc since: "3.0.0"
#   @moduledoc false

#   @behaviour EctoShorts.Generator.Builder

#   alias EctoShorts.Generator.AST
#   alias EctoShorts.Generator.Blueprint

#   @doc false
#   @impl true
#   def specs_for(binding_head_ast, binding_body_asts, target_binding_var, context) do
#     lower_upper_specsspecs_for(binding_head_ast, binding_body_asts, target_binding_var, context)
#   end

#   @doc false
#   def lower_upper_specsspecs_for(binding_head_ast, binding_body_asts, target_binding_var, context) do
#     key_var = Macro.var(:key, context)
#     value_var = Macro.var(:value, context)
#     field_ast = AST.field_ast(target_binding_var, key_var)

#     [
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:not, {:==, {:lower, unquote(value_var)}}}),
#         body:
#           quote do
#             compose(
#               unquote(binding_head_ast),
#               unquote(key_var),
#               {:not, {:lower, unquote(value_var)}}
#             )
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:not, {:==, {:upper, unquote(value_var)}}}),
#         body:
#           quote do
#             compose(
#               unquote(binding_head_ast),
#               unquote(key_var),
#               {:not, {:upper, unquote(value_var)}}
#             )
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:not, {:!=, {:lower, unquote(value_var)}}}),
#         body:
#           quote do
#             compose(unquote(binding_head_ast), unquote(key_var), {:lower, unquote(value_var)})
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:not, {:!=, {:upper, unquote(value_var)}}}),
#         body:
#           quote do
#             compose(unquote(binding_head_ast), unquote(key_var), {:upper, unquote(value_var)})
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:not, {:lower, unquote(value_var)}}),
#         body:
#           AST.dynamic_ast(
#             binding_body_asts,
#             quote do
#               fragment(
#                 "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
#                 unquote(field_ast),
#                 ^unquote(value_var)
#               )
#             end
#           )
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:not, {:upper, unquote(value_var)}}),
#         body:
#           AST.dynamic_ast(
#             binding_body_asts,
#             quote do
#               fragment(
#                 "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
#                 unquote(field_ast),
#                 ^unquote(value_var)
#               )
#             end
#           )
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:==, {:lower, unquote(value_var)}}),
#         body:
#           quote do
#             compose(unquote(binding_head_ast), unquote(key_var), {:lower, unquote(value_var)})
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:==, {:upper, unquote(value_var)}}),
#         body:
#           quote do
#             compose(unquote(binding_head_ast), unquote(key_var), {:upper, unquote(value_var)})
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:!=, {:lower, unquote(value_var)}}),
#         body:
#           quote do
#             compose(
#               unquote(binding_head_ast),
#               unquote(key_var),
#               {:not, {:lower, unquote(value_var)}}
#             )
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:!=, {:upper, unquote(value_var)}}),
#         body:
#           quote do
#             compose(
#               unquote(binding_head_ast),
#               unquote(key_var),
#               {:not, {:upper, unquote(value_var)}}
#             )
#           end
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:lower, unquote(value_var)}),
#         body:
#           AST.dynamic_ast(
#             binding_body_asts,
#             quote do
#               fragment(
#                 "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
#                 unquote(field_ast),
#                 ^unquote(value_var)
#               )
#             end
#           )
#       },
#       %Blueprint{
#         binding_head: binding_head_ast,
#         key: key_var,
#         head: quote(do: {:upper, unquote(value_var)}),
#         body:
#           AST.dynamic_ast(
#             binding_body_asts,
#             quote do
#               fragment(
#                 "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
#                 unquote(field_ast),
#                 ^unquote(value_var)
#               )
#             end
#           )
#       }
#     ]
#   end
# end
