defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs do
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec

  @aggregate_operators [:avg, :count, :max, :min, :sum]
  @comparison_operators [:==, :!=, :>, :>=, :<, :<=]
  @comparison_alias_operators [:eq, :gt, :gte, :lt, :lte]

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    list_semantic_specs(context, binding_head_ast) ++
      alias_op_specs(context, binding_head_ast) ++
      nil_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      aggregate_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
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
                unquote(
                  AST.dynamic_ast(binding_body_asts, quote(do: not is_nil(unquote(field_ast))))
                )

              _ ->
                raise ArgumentError,
                  message:
                    "Expected the operator to be one of [:eq, :==, :!=] for nil comparison, got: #{inspect(unquote(op_var))}"
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

    default_value_guard =
      quote do
        not is_tuple(unquote(value_var))
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
          head: quote(do: {unquote(helper), unquote(value_var)}),
          guard: default_value_guard,
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
          guard: default_value_guard,
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(helper), {:!=, unquote(value_var)}}
              )
            end
        },
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
end
