defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Specs.Aggregate do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec
  require EctoShorts.Dynamics.Postgres.ExprHelpers
  alias EctoShorts.Dynamics.Postgres.ExprHelpers

  @aggregate_operators [:avg, :count, :max, :min, :sum]
  @comparison_operators [:==, :!=, :>, :>=, :<, :<=]
  @comparison_alias_operators [:eq, :ne, :gt, :gte, :lt, :lte]

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    aggregate_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def aggregate_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
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

    Enum.flat_map(@aggregate_operators, fn helper ->
      aggregate_expr_ast =
        case helper do
          :avg -> quote(do: avg(unquote(field_ast)))
          :count -> quote(do: count(unquote(field_ast)))
          :max -> quote(do: max(unquote(field_ast)))
          :min -> quote(do: min(unquote(field_ast)))
          :sum -> quote(do: sum(unquote(field_ast)))
        end

      [
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {unquote(helper), {unquote(op_var), unquote(value_var)}}),
          guard: alias_guard,
          body:
            quote do
              mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

              compose(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(helper), {mapped_op, unquote(value_var)}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, {unquote(helper), {unquote(op_var), unquote(value_var)}}}),
          guard: alias_guard,
          body:
            quote do
              mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

              compose(
                unquote(binding_head_ast),
                unquote(key_var),
                {:not, {unquote(helper), {mapped_op, unquote(value_var)}}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {unquote(helper), {unquote(op_var), unquote(value_var)}}),
          guard: op_guard,
          body:
            ExprHelpers.aggregate_dynamic_case_ast(
              binding_body_asts,
              aggregate_expr_ast,
              op_var,
              value_var
            )
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, {unquote(helper), {unquote(op_var), unquote(value_var)}}}),
          guard: op_guard,
          body:
            ExprHelpers.not_aggregate_dynamic_case_ast(
              binding_body_asts,
              aggregate_expr_ast,
              op_var,
              value_var
            )
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {unquote(helper), unquote(value_var)}),
          body:
            quote do
              compose(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(helper), {:==, unquote(value_var)}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, {unquote(helper), unquote(value_var)}}),
          body:
            quote do
              compose(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(helper), {:!=, unquote(value_var)}}
              )
            end
        }
      ]
    end)
  end
end
