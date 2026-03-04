defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.Arithmetic do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec

  @arithmetic_operators [:+, :-, :*, :/]
  @comparison_operators [:==, :!=, :>, :>=, :<, :<=]

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    arithmetic_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def arithmetic_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    left_var = Macro.var(:left, context)
    right_var = Macro.var(:right, context)
    rhs_dynamic_var = Macro.var(:rhs_dynamic, context)

    field_ast = AST.field_ast(target_binding_var, key_var)

    Enum.flat_map(@comparison_operators, fn op ->
      Enum.flat_map(@arithmetic_operators, fn arithmetic_op ->
        arithmetic_expr_ast =
          quote(do: {unquote(arithmetic_op), [unquote(left_var), unquote(right_var)]})

        rhs_dynamic_expr_ast =
          arithmetic_dynamic_expr_ast(
            binding_body_asts,
            target_binding_var,
            arithmetic_expr_ast
          )

        comparison_ast = comparison_dynamic_expr_ast(field_ast, op, rhs_dynamic_var)

        [
          %ClauseSpec{
            binding_head: binding_head_ast,
            key: key_var,
            head: quote(do: {unquote(op), {unquote(arithmetic_op), [unquote(left_var), unquote(right_var)]}}),
            body:
              quote do
                unquote(rhs_dynamic_var) = unquote(rhs_dynamic_expr_ast)

                unquote(
                  AST.dynamic_ast(
                    binding_body_asts,
                    comparison_ast
                  )
                )
              end
          },
          %ClauseSpec{
            binding_head: binding_head_ast,
            key: key_var,
            head:
              quote(
                do: {:not, {unquote(op), {unquote(arithmetic_op), [unquote(left_var), unquote(right_var)]}}}
              ),
            body:
              quote do
                unquote(rhs_dynamic_var) = unquote(rhs_dynamic_expr_ast)

                unquote(
                  AST.dynamic_ast(
                    binding_body_asts,
                    AST.not_ast(comparison_ast)
                  )
                )
              end
          }
        ]
      end)
    end)
  end

  defp comparison_dynamic_expr_ast(field_ast, op, rhs_dynamic_var) do
    case op do
      :== -> quote(do: unquote(field_ast) == ^unquote(rhs_dynamic_var))
      :!= -> quote(do: unquote(field_ast) != ^unquote(rhs_dynamic_var))
      :> -> quote(do: unquote(field_ast) > ^unquote(rhs_dynamic_var))
      :>= -> quote(do: unquote(field_ast) >= ^unquote(rhs_dynamic_var))
      :< -> quote(do: unquote(field_ast) < ^unquote(rhs_dynamic_var))
      :<= -> quote(do: unquote(field_ast) <= ^unquote(rhs_dynamic_var))
    end
  end

  defp arithmetic_dynamic_expr_ast(binding_body_asts, target_binding_var, arithmetic_expr_ast) do
    quote do
      outer_func = fn inner_func, expr ->
        case expr do
          {op, [left_expr, right_expr]} when op in unquote(@arithmetic_operators) ->
            left_dynamic = inner_func.(inner_func, left_expr)
            right_dynamic = inner_func.(inner_func, right_expr)

            case op do
              :+ ->
                unquote(
                  AST.dynamic_ast(
                    binding_body_asts,
                    quote(do: ^left_dynamic + ^right_dynamic)
                  )
                )

              :- ->
                unquote(
                  AST.dynamic_ast(
                    binding_body_asts,
                    quote(do: ^left_dynamic - ^right_dynamic)
                  )
                )

              :* ->
                unquote(
                  AST.dynamic_ast(
                    binding_body_asts,
                    quote(do: ^left_dynamic * ^right_dynamic)
                  )
                )

              :/ ->
                unquote(
                  AST.dynamic_ast(
                    binding_body_asts,
                    quote(do: ^left_dynamic / ^right_dynamic)
                  )
                )
            end

          field_name when is_atom(field_name) ->
            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  field(unquote(target_binding_var), ^field_name)
                end
              )
            )

          literal ->
            unquote(AST.dynamic_ast(binding_body_asts, quote(do: ^literal)))
        end
      end

      outer_func.(outer_func, unquote(arithmetic_expr_ast))
    end
  end
end
