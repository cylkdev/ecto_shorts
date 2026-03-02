defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Quantifier do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec
  alias EctoShorts.Dynamics.Adapters.Postgres.ExprHelpers

  @comparison_operators [:==, :!=, :>, :>=, :<, :<=]
  @comparison_alias_operators [:eq, :ne, :gt, :gte, :lt, :lte]

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    all_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      any_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def all_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    value_var = Macro.var(:value, context)

    field_ast = AST.field_ast(target_binding_var, key_var)

    op_guard =
      quote do
        unquote(op_var) in unquote(@comparison_operators)
      end

    alias_guard =
      quote do
        unquote(op_var) in unquote(@comparison_alias_operators)
      end

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), {:all, unquote(value_var)}}),
        guard: op_guard,
        body: all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {unquote(op_var), {:all, unquote(value_var)}}}),
        guard: op_guard,
        body: not_all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), {:all, unquote(value_var)}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {mapped_op, {:all, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {unquote(op_var), {:all, unquote(value_var)}}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {mapped_op, {:all, unquote(value_var)}}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:all, unquote(value_var)}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:==, {:all, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:all, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:!=, {:all, unquote(value_var)}}
            )
          end
      }
    ]
  end

  @doc false
  def any_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    value_var = Macro.var(:value, context)

    field_ast = AST.field_ast(target_binding_var, key_var)

    op_guard =
      quote do
        unquote(op_var) in unquote(@comparison_operators)
      end

    alias_guard =
      quote do
        unquote(op_var) in unquote(@comparison_alias_operators)
      end

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), {:any, unquote(value_var)}}),
        guard: op_guard,
        body: any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {unquote(op_var), {:any, unquote(value_var)}}}),
        guard: op_guard,
        body: not_any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), {:any, unquote(value_var)}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {mapped_op, {:any, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {unquote(op_var), {:any, unquote(value_var)}}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {mapped_op, {:any, unquote(value_var)}}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:any, unquote(value_var)}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:==, {:any, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:any, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:!=, {:any, unquote(value_var)}}
            )
          end
      }
    ]
  end

  defp all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) == all(unquote(value_var)))))
        :!= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) != all(unquote(value_var)))))
        :> -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) > all(unquote(value_var)))))
        :>= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) >= all(unquote(value_var)))))
        :< -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) < all(unquote(value_var)))))
        :<= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) <= all(unquote(value_var)))))
      end
    end
  end

  defp not_all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) == all(unquote(value_var))))))
        :!= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) != all(unquote(value_var))))))
        :> -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) > all(unquote(value_var))))))
        :>= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) >= all(unquote(value_var))))))
        :< -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) < all(unquote(value_var))))))
        :<= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) <= all(unquote(value_var))))))
      end
    end
  end

  defp any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) == any(unquote(value_var)))))
        :!= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) != any(unquote(value_var)))))
        :> -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) > any(unquote(value_var)))))
        :>= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) >= any(unquote(value_var)))))
        :< -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) < any(unquote(value_var)))))
        :<= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) <= any(unquote(value_var)))))
      end
    end
  end

  defp not_any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) == any(unquote(value_var))))))
        :!= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) != any(unquote(value_var))))))
        :> -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) > any(unquote(value_var))))))
        :>= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) >= any(unquote(value_var))))))
        :< -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) < any(unquote(value_var))))))
        :<= -> unquote(AST.dynamic_ast(binding_body_asts, quote(do: not (unquote(field_ast) <= any(unquote(value_var))))))
      end
    end
  end
end
