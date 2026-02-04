defmodule EctoShorts.QueryBuilder.Dynamics.Expressions.Postgres.Specs.ArrayExprs do
  @moduledoc false

  alias EctoShorts.QueryBuilder.BindingHelpers
  alias EctoShorts.QueryBuilder.Dynamics.Expression.AST
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilder
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseSpec
  alias EctoShorts.QueryBuilder.Dynamics.Expression.Emitters.DynamicFieldExpr

  @doc """
  Defines array `dynamic_field_expr/3` clauses.
  """
  defmacro define_exprs(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    clause_asts =
      for {binding_head_ast, binding_body_asts} <- binding_patterns,
          spec <-
            clause_specs(
              :array,
              context,
              binding_head_ast,
              target_binding_var,
              binding_body_asts
            ) do
        clause_ast_or_raise(DynamicFieldExpr, spec)
      end

    quote do
      (unquote_splicing(clause_asts))
    end
  end

  @doc false
  def clause_specs(
        :array = kind,
        context,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) do
    list_semantic_specs(kind, context, binding_head_ast) ++
      alias_op_specs(kind, context, binding_head_ast) ++
      nil_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) ++
      lower_upper_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) ++
      like_ilike_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) ++
      base_op_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  def clause_specs(_kind, _context, _binding_head_ast, _target_binding_var, _binding_body_asts) do
    []
  end

  @doc false
  def list_semantic_specs(kind, context, binding_head_ast) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    values_var = Macro.var(:values, context)

    list_guard =
      quote do
        is_list(unquote(values_var))
      end

    [
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:==, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:!=, unquote(values_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, unquote(values_var)}}),
        guard: list_guard,
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:==, unquote(values_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:==, unquote(value_var)}}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:!=, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, unquote(value_var)}}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:==, unquote(value_var)}
            )
          end
      }
    ]
  end

  @doc false
  def alias_op_specs(kind, context, binding_head_ast) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    value_var = Macro.var(:value, context)

    op_guard =
      quote do
        unquote(op_var) in [:gt, :gte, :lt, :lte, :eq]
      end

    [
      %ClauseSpec{
        kind: kind,
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

            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {mapped_op, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
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

            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {mapped_op, unquote(value_var)}}
            )
          end
      }
    ]
  end

  @doc false
  def nil_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        kind: kind,
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
  def lower_upper_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:lower, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment(
                """
                NOT EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE lower(t) = ?
                )
                """,
                unquote(field_ast),
                ^unquote(value_var)
              )
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:upper, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment(
                """
                NOT EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE upper(t) = ?
                )
                """,
                unquote(field_ast),
                ^unquote(value_var)
              )
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, {:lower, unquote(value_var)}}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:lower, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, {:upper, unquote(value_var)}}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:upper, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:lower, unquote(value_var)}}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:lower, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:upper, unquote(value_var)}}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:upper, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:lower, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment(
                """
                EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE lower(t) = ?
                )
                """,
                unquote(field_ast),
                ^unquote(value_var)
              )
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:upper, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment(
                """
                EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE upper(t) = ?
                )
                """,
                unquote(field_ast),
                ^unquote(value_var)
              )
            end
          )
      }
    ]
  end

  @doc false
  def like_ilike_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:ilike, unquote(value_var)}}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  fragment(
                    """
                    NOT EXISTS (
                      SELECT 1
                      FROM unnest(?) AS t
                      WHERE t ILIKE ANY (?)
                    )
                    """,
                    unquote(field_ast),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:like, unquote(value_var)}}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  fragment(
                    """
                    NOT EXISTS (
                      SELECT 1
                      FROM unnest(?) AS t
                      WHERE t LIKE ANY (?)
                    )
                    """,
                    unquote(field_ast),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:ilike, unquote(value_var)}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  fragment(
                    """
                    EXISTS (
                      SELECT 1
                      FROM unnest(?) AS t
                      WHERE t ILIKE ANY (?)
                    )
                    """,
                    unquote(field_ast),
                    ^patterns
                  )
                end
              )
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:like, unquote(value_var)}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  fragment(
                    """
                    EXISTS (
                      SELECT 1
                      FROM unnest(?) AS t
                      WHERE t LIKE ANY (?)
                    )
                    """,
                    unquote(field_ast),
                    ^patterns
                  )
                end
              )
            )
          end
      }
    ]
  end

  @doc false
  def base_op_specs(kind, context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    values_var = Macro.var(:values, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    list_guard =
      quote do
        is_list(unquote(values_var))
      end

    [
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:in, {:all, unquote(values_var)}}}),
        guard: list_guard,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not fragment("? @> ?", unquote(field_ast), ^unquote(values_var))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:in, unquote(values_var)}}),
        guard: list_guard,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not fragment("? && ?", unquote(field_ast), ^unquote(values_var))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:in, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              ^unquote(value_var) not in unquote(field_ast)
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:>, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not fragment("? < ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:>=, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not fragment("? <= ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:<, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not fragment("? > ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:<=, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              not fragment("? >= ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, unquote(values_var)}),
        guard: list_guard,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote(do: unquote(field_ast) == ^unquote(values_var))
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, unquote(values_var)}),
        guard: list_guard,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote(do: unquote(field_ast) != ^unquote(values_var))
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, unquote(value_var)}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:in, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:in, unquote(values_var)}),
        guard: list_guard,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("? && ?", unquote(field_ast), ^unquote(values_var))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:in, {:all, unquote(values_var)}}),
        guard: list_guard,
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("? @> ?", unquote(field_ast), ^unquote(values_var))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:>, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("? < ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:>=, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("? <= ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:<, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("? > ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:<=, unquote(value_var)}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment("? >= ANY(?)", ^unquote(value_var), unquote(field_ast))
            end
          )
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, unquote(value_var)}),
        body:
          quote do
            dynamic_field_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:in, unquote(value_var)}
            )
          end
      },
      %ClauseSpec{
        kind: kind,
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:in, unquote(value_var)}),
        body:
          AST.dynamic_ast(binding_body_asts, quote(do: ^unquote(value_var) in unquote(field_ast)))
      }
    ]
  end

  defp clause_ast_or_raise(emitter, spec) when is_atom(emitter) do
    case ClauseBuilder.clause_ast(emitter, spec) do
      {:ok, ast} -> ast
      {:error, reason} -> raise ArgumentError, "Failed to build clause: #{inspect(reason)}"
    end
  end
end
