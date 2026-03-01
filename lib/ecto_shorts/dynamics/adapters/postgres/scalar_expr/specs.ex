defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec

  @aggregate_operators [:avg, :count, :max, :min, :sum]
  @arithmetic_operators [:+, :-, :*, :/]
  @comparison_operators [:==, :!=, :>, :>=, :<, :<=]
  @comparison_alias_operators [:eq, :gt, :gte, :lt, :lte]
  @date_time_helpers [
    {:datetime, :add},
    {:datetime, :ago},
    {:datetime, :from_now},
    {:date, :add}
  ]

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    list_semantic_specs(context, binding_head_ast) ++
      alias_op_specs(context, binding_head_ast) ++
      nil_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      aggregate_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      arithmetic_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      date_time_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      all_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      any_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      lower_upper_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      base_op_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def list_semantic_specs(context, binding_head_ast) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    values_var = Macro.var(:values, context)

    list_guard =
      quote do
        is_list(unquote(values_var))
      end

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:==, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:in, unquote(values_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:in, unquote(values_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, unquote(values_var)}),
        guard: list_guard,
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:in, unquote(values_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, unquote(values_var)}),
        guard: list_guard,
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:in, unquote(values_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:==, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:!=, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:==, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:in, {:all, unquote(values_var)}}}),
        guard: list_guard,
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:in, unquote(values_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:in, {:all, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:in, unquote(values_var)}
            )
          end
      }
    ]
  end

  @doc false
  def alias_op_specs(context, binding_head_ast) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    value_var = Macro.var(:value, context)

    op_guard =
      quote do
        unquote(op_var) in [:gt, :gte, :lt, :lte, :eq]
      end

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), unquote(value_var)}),
        guard: op_guard,
        body:
          quote do
            mapped_op =
              case unquote(op_var) do
                :gt -> :>
                :gte -> :>=
                :lt -> :<
                :lte -> :<=
                :eq -> :==
              end

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {mapped_op, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {unquote(op_var), unquote(value_var)}}),
        guard: op_guard,
        body:
          quote do
            mapped_op =
              case unquote(op_var) do
                :gt -> :>
                :gte -> :>=
                :lt -> :<
                :lte -> :<=
                :eq -> :==
              end

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {mapped_op, unquote(value_var)}}
            )
          end
      }
    ]
  end

  @doc false
  def nil_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)

    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), nil}),
        body:
          quote do
            case unquote(op_var) do
              :eq ->
                unquote(AST.dynamic_ast(binding_body_asts, quote(do: is_nil(unquote(field_ast)))))

              :== ->
                unquote(AST.dynamic_ast(binding_body_asts, quote(do: is_nil(unquote(field_ast)))))

              :!= ->
                unquote(AST.dynamic_ast(binding_body_asts, quote(do: not is_nil(unquote(field_ast)))))

              _ ->
                nil
            end
          end
      }
    ]
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
              mapped_op =
                case unquote(op_var) do
                  :eq -> :==
                  :gt -> :>
                  :gte -> :>=
                  :lt -> :<
                  :lte -> :<=
                end

              apply_dynamic_expr(
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
              mapped_op =
                case unquote(op_var) do
                  :eq -> :==
                  :gt -> :>
                  :gte -> :>=
                  :lt -> :<
                  :lte -> :<=
                end

              apply_dynamic_expr(
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
            aggregate_dynamic_case_ast(
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
            not_aggregate_dynamic_case_ast(
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
              apply_dynamic_expr(
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
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(helper), {:!=, unquote(value_var)}}
              )
            end
        }
      ]
    end)
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
        head: quote(do: {:all, {unquote(op_var), unquote(value_var)}}),
        guard: op_guard,
        body: all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:all, {unquote(op_var), unquote(value_var)}}}),
        guard: op_guard,
        body: not_all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:all, {unquote(op_var), unquote(value_var)}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op =
              case unquote(op_var) do
                :gt -> :>
                :gte -> :>=
                :lt -> :<
                :lte -> :<=
                :eq -> :==
              end

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:all, {mapped_op, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:all, {unquote(op_var), unquote(value_var)}}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op =
              case unquote(op_var) do
                :gt -> :>
                :gte -> :>=
                :lt -> :<
                :lte -> :<=
                :eq -> :==
              end

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:all, {mapped_op, unquote(value_var)}}}
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
              {:all, {:==, unquote(value_var)}}
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
              {:all, {:!=, unquote(value_var)}}
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
        head: quote(do: {:any, {unquote(op_var), unquote(value_var)}}),
        guard: op_guard,
        body: any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:any, {unquote(op_var), unquote(value_var)}}}),
        guard: op_guard,
        body: not_any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:any, {unquote(op_var), unquote(value_var)}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op =
              case unquote(op_var) do
                :gt -> :>
                :gte -> :>=
                :lt -> :<
                :lte -> :<=
                :eq -> :==
              end

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:any, {mapped_op, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:any, {unquote(op_var), unquote(value_var)}}}),
        guard: alias_guard,
        body:
          quote do
            mapped_op =
              case unquote(op_var) do
                :gt -> :>
                :gte -> :>=
                :lt -> :<
                :lte -> :<=
                :eq -> :==
              end

            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:any, {mapped_op, unquote(value_var)}}}
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
              {:any, {:==, unquote(value_var)}}
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
              {:any, {:!=, unquote(value_var)}}
            )
          end
      }
    ]
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

  @doc false
  def date_time_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    payload_var = Macro.var(:payload, context)
    rhs_dynamic_var = Macro.var(:rhs_dynamic, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    op_guard =
      quote do
        unquote(op_var) in unquote(@comparison_operators)
      end

    Enum.flat_map(@date_time_helpers, fn {wrapper, operation} ->
      helper_expr_ast =
        quote(do: {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]})

      rhs_dynamic_expr_ast =
        date_time_dynamic_expr_ast(binding_body_asts, target_binding_var, helper_expr_ast)

      [
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(do: {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}),
          guard: op_guard,
          body:
            quote do
              unquote(rhs_dynamic_var) = unquote(rhs_dynamic_expr_ast)

              unquote(
                date_time_comparison_case_ast(
                  binding_body_asts,
                  field_ast,
                  op_var,
                  rhs_dynamic_var
                )
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(
              do: {:not, {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}}
            ),
          guard: op_guard,
          body:
            quote do
              unquote(rhs_dynamic_var) = unquote(rhs_dynamic_expr_ast)

              unquote(
                not_date_time_comparison_case_ast(
                  binding_body_asts,
                  field_ast,
                  op_var,
                  rhs_dynamic_var
                )
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:==, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:!=, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(do: {unquote(op_var), %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}}),
          guard: op_guard,
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(
              do:
                {:not,
                 {unquote(op_var), %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}}}
            ),
          guard: op_guard,
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:not, {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:==, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:!=, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        }
      ]
    end)
  end

  @doc false
  def lower_upper_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, {:lower, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:lower, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, {:upper, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:upper, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:lower, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("lower(?)", unquote(field_ast)) == ^unquote(value_var)
            end
          )
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:upper, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("upper(?)", unquote(field_ast)) == ^unquote(value_var)
            end
          )
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:lower, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not (fragment("lower(?)", unquote(field_ast)) == ^unquote(value_var))
            end
          )
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:upper, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not (fragment("upper(?)", unquote(field_ast)) == ^unquote(value_var))
            end
          )
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:lower, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("lower(?)", unquote(field_ast)) != ^unquote(value_var)
            end
          )
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:upper, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("upper(?)", unquote(field_ast)) != ^unquote(value_var)
            end
          )
      }
    ]
  end

  @doc false
  def like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    values_var = Macro.var(:values, context)

    list_guard =
      quote do
        is_list(unquote(values_var))
      end

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:like, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            patterns = unquote(AST.list_of_patterns_ast(values_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  not fragment(
                    "? LIKE ANY(?)",
                    unquote(AST.field_ast(target_binding_var, key_var)),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:like, unquote(value_var)}}),
        body:
          quote do
            search_query = unquote(AST.search_query_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  not like(unquote(AST.field_ast(target_binding_var, key_var)), ^search_query)
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:ilike, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            patterns = unquote(AST.list_of_patterns_ast(values_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  not fragment(
                    "? ILIKE ANY(?)",
                    unquote(AST.field_ast(target_binding_var, key_var)),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:ilike, unquote(value_var)}}),
        body:
          quote do
            search_query = unquote(AST.search_query_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  not ilike(unquote(AST.field_ast(target_binding_var, key_var)), ^search_query)
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:like, unquote(values_var)}),
        guard: list_guard,
        body:
          quote do
            patterns = unquote(AST.list_of_patterns_ast(values_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  fragment(
                    "? LIKE ANY(?)",
                    unquote(AST.field_ast(target_binding_var, key_var)),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:like, unquote(value_var)}),
        body:
          quote do
            search_query = unquote(AST.search_query_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  like(unquote(AST.field_ast(target_binding_var, key_var)), ^search_query)
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:ilike, unquote(values_var)}),
        guard: list_guard,
        body:
          quote do
            patterns = unquote(AST.list_of_patterns_ast(values_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  fragment(
                    "? ILIKE ANY(?)",
                    unquote(AST.field_ast(target_binding_var, key_var)),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:ilike, unquote(value_var)}),
        body:
          quote do
            search_query = unquote(AST.search_query_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  ilike(unquote(AST.field_ast(target_binding_var, key_var)), ^search_query)
                end
              )
            )
          end
      }
    ]
  end

  @doc false
  def base_op_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)

    field_ast = AST.field_ast(target_binding_var, key_var)

    direct_ops =
      for op <- [:==, :!=, :in, :>, :>=, :<, :<=] do
        expr_ast =
          case op do
            :== -> quote(do: unquote(field_ast) == ^unquote(value_var))
            :!= -> quote(do: unquote(field_ast) != ^unquote(value_var))
            :in -> quote(do: unquote(field_ast) in ^unquote(value_var))
            :> -> quote(do: unquote(field_ast) > ^unquote(value_var))
            :>= -> quote(do: unquote(field_ast) >= ^unquote(value_var))
            :< -> quote(do: unquote(field_ast) < ^unquote(value_var))
            :<= -> quote(do: unquote(field_ast) <= ^unquote(value_var))
          end

        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {unquote(op), unquote(value_var)}),
          body: AST.dynamic_ast(binding_body_asts, expr_ast)
        }
      end

    not_ops =
      for op <- [:in, :>, :>=, :<, :<=] do
        expr_ast =
          case op do
            :in -> quote(do: unquote(field_ast) not in ^unquote(value_var))
            :> -> quote(do: not (unquote(field_ast) > ^unquote(value_var)))
            :>= -> quote(do: not (unquote(field_ast) >= ^unquote(value_var)))
            :< -> quote(do: not (unquote(field_ast) < ^unquote(value_var)))
            :<= -> quote(do: not (unquote(field_ast) <= ^unquote(value_var)))
          end

        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, {unquote(op), unquote(value_var)}}),
          body: AST.dynamic_ast(binding_body_asts, expr_ast)
        }
      end

    direct_ops ++ not_ops
  end

  defp aggregate_dynamic_case_ast(binding_body_asts, aggregate_expr_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) == ^unquote(value_var))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) != ^unquote(value_var))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) > ^unquote(value_var))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) >= ^unquote(value_var))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) < ^unquote(value_var))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(aggregate_expr_ast) <= ^unquote(value_var))
            )
          )
      end
    end
  end

  defp not_aggregate_dynamic_case_ast(binding_body_asts, aggregate_expr_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) == ^unquote(value_var)))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) != ^unquote(value_var)))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) > ^unquote(value_var)))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) >= ^unquote(value_var)))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) < ^unquote(value_var)))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(aggregate_expr_ast) <= ^unquote(value_var)))
            )
          )
      end
    end
  end

  defp all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) == all(unquote(value_var)))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) != all(unquote(value_var)))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) > all(unquote(value_var)))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) >= all(unquote(value_var)))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) < all(unquote(value_var)))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) <= all(unquote(value_var)))
            )
          )
      end
    end
  end

  defp not_all_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) == all(unquote(value_var))))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) != all(unquote(value_var))))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) > all(unquote(value_var))))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) >= all(unquote(value_var))))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) < all(unquote(value_var))))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) <= all(unquote(value_var))))
            )
          )
      end
    end
  end

  defp any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) == any(unquote(value_var)))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) != any(unquote(value_var)))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) > any(unquote(value_var)))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) >= any(unquote(value_var)))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) < any(unquote(value_var)))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) <= any(unquote(value_var)))
            )
          )
      end
    end
  end

  defp not_any_dynamic_case_ast(binding_body_asts, field_ast, op_var, value_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) == any(unquote(value_var))))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) != any(unquote(value_var))))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) > any(unquote(value_var))))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) >= any(unquote(value_var))))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) < any(unquote(value_var))))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) <= any(unquote(value_var))))
            )
          )
      end
    end
  end

  defp date_time_comparison_case_ast(binding_body_asts, field_ast, op_var, rhs_dynamic_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) == ^unquote(rhs_dynamic_var))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) != ^unquote(rhs_dynamic_var))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) > ^unquote(rhs_dynamic_var))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) >= ^unquote(rhs_dynamic_var))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) < ^unquote(rhs_dynamic_var))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: unquote(field_ast) <= ^unquote(rhs_dynamic_var))
            )
          )
      end
    end
  end

  defp not_date_time_comparison_case_ast(binding_body_asts, field_ast, op_var, rhs_dynamic_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) == ^unquote(rhs_dynamic_var)))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) != ^unquote(rhs_dynamic_var)))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) > ^unquote(rhs_dynamic_var)))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) >= ^unquote(rhs_dynamic_var)))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) < ^unquote(rhs_dynamic_var)))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) <= ^unquote(rhs_dynamic_var)))
            )
          )
      end
    end
  end

  defp date_time_dynamic_expr_ast(binding_body_asts, target_binding_var, helper_expr_ast) do
    quote do
      normalize_payload = fn
        label, payload when is_map(payload) and not is_struct(payload) ->
          payload

        label, payload when is_list(payload) ->
          if Keyword.keyword?(payload) do
            Map.new(payload)
          else
            raise ArgumentError,
                  "Expected #{inspect(label)} payload to be a map or keyword list, got: #{inspect(payload)}"
          end

        label, payload ->
          raise ArgumentError,
                "Expected #{inspect(label)} payload to be a map or keyword list, got: #{inspect(payload)}"
      end

      build_date_time_expr = fn build_date_time_expr, expr ->
        case expr do
          {:datetime, [{:from_now, payload}]} ->
            payload = normalize_payload.({:datetime, :from_now}, payload)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            unquote(AST.dynamic_ast(binding_body_asts, quote(do: from_now(^count, ^interval))))

          {:datetime, [{:ago, payload}]} ->
            payload = normalize_payload.({:datetime, :ago}, payload)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            unquote(AST.dynamic_ast(binding_body_asts, quote(do: ago(^count, ^interval))))

          {:datetime, [{:add, payload}]} ->
            payload = normalize_payload.({:datetime, :add}, payload)
            field_expr = Map.fetch!(payload, :field)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            field_dynamic = build_date_time_expr.(build_date_time_expr, field_expr)

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote(do: datetime_add(^field_dynamic, ^count, ^interval))
              )
            )

          {:date, [{:add, payload}]} ->
            payload = normalize_payload.({:date, :add}, payload)
            field_expr = Map.fetch!(payload, :field)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            field_dynamic = build_date_time_expr.(build_date_time_expr, field_expr)

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote(do: date_add(^field_dynamic, ^count, ^interval))
              )
            )

          %{datetime: inner} when is_map(inner) and map_size(inner) === 1 ->
            inner_list = Map.to_list(inner)
            build_date_time_expr.(build_date_time_expr, {:datetime, inner_list})

          %{date: inner} when is_map(inner) and map_size(inner) === 1 ->
            inner_list = Map.to_list(inner)
            build_date_time_expr.(build_date_time_expr, {:date, inner_list})

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

      build_date_time_expr.(build_date_time_expr, unquote(helper_expr_ast))
    end
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
