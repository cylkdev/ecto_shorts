defmodule EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.Core do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec
  alias EctoShorts.Dynamics.Adapters.Postgres.ExprHelpers

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    list_semantic_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      alias_op_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      nil_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      lower_upper_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts) ++
      base_op_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def list_semantic_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
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
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:==, unquote(values_var)}}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) != ^unquote(values_var)))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, unquote(values_var)}}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) == ^unquote(values_var)))
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
      }
    ]
  end

  @doc false
  def alias_op_specs(context, binding_head_ast, _target_binding_var, _binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    value_var = Macro.var(:value, context)

    op_guard =
      quote do
        unquote(op_var) in [:gt, :gte, :lt, :lte, :eq, :ne]
      end

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {unquote(op_var), unquote(value_var)}),
        guard: op_guard,
        body:
          quote do
            mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

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
            mapped_op = unquote(ExprHelpers.alias_to_canonical_map_ast(op_var))

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

              :ne ->
                unquote(AST.dynamic_ast(binding_body_asts, quote(do: not is_nil(unquote(field_ast)))))

              _ ->
                nil
            end
          end
      }
    ]
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
        head: quote(do: {:not, {:lower, unquote(value_var)}}),
        body:
          AST.dynamic_ast(binding_body_asts, quote do
            fragment("NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n", unquote(field_ast), ^unquote(value_var))
          end)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:upper, unquote(value_var)}}),
        body:
          AST.dynamic_ast(binding_body_asts, quote do
            fragment("NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n", unquote(field_ast), ^unquote(value_var))
          end)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, {:lower, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:lower, unquote(value_var)})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, {:upper, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:upper, unquote(value_var)})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:lower, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:not, {:lower, unquote(value_var)}})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:upper, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:not, {:upper, unquote(value_var)}})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:lower, unquote(value_var)}),
        body:
          AST.dynamic_ast(binding_body_asts, quote do
            fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n", unquote(field_ast), ^unquote(value_var))
          end)
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:upper, unquote(value_var)}),
        body:
          AST.dynamic_ast(binding_body_asts, quote do
            fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n", unquote(field_ast), ^unquote(value_var))
          end)
      }
    ]
  end

  @doc false
  def like_ilike_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    [
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:ilike, unquote(value_var)}}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:like, unquote(value_var)}}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:ilike, unquote(value_var)}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:like, unquote(value_var)}),
        body:
          quote do
            patterns = unquote(AST.normalize_patterns_ast(value_var))

            unquote(AST.dynamic_ast(binding_body_asts, quote do
              fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n", unquote(field_ast), ^patterns)
            end))
          end
      }
    ]
  end

  @doc false
  def base_op_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
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
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:all, {:in, unquote(values_var)}}}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: not fragment("? @> ?", unquote(field_ast), ^unquote(values_var))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:in, unquote(values_var)}}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: not fragment("? && ?", unquote(field_ast), ^unquote(values_var))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:in, unquote(value_var)}}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: ^unquote(value_var) not in unquote(field_ast)))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:>, unquote(value_var)}}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: not fragment("? < ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:>=, unquote(value_var)}}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: not fragment("? <= ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:<, unquote(value_var)}}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: not fragment("? > ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:<=, unquote(value_var)}}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: not fragment("? >= ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, unquote(values_var)}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) == ^unquote(values_var)))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, unquote(values_var)}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) != ^unquote(values_var)))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, unquote(value_var)}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:not, {:in, unquote(value_var)}})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:in, unquote(values_var)}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: fragment("? && ?", unquote(field_ast), ^unquote(values_var))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:all, {:in, unquote(values_var)}}),
        guard: list_guard,
        body: AST.dynamic_ast(binding_body_asts, quote(do: fragment("? @> ?", unquote(field_ast), ^unquote(values_var))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:>, unquote(value_var)}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: fragment("? < ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:>=, unquote(value_var)}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: fragment("? <= ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:<, unquote(value_var)}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: fragment("? > ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:<=, unquote(value_var)}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: fragment("? >= ANY(?)", ^unquote(value_var), unquote(field_ast))))
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:==, unquote(value_var)}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:in, unquote(value_var)})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:in, unquote(value_var)}),
        body: AST.dynamic_ast(binding_body_asts, quote(do: ^unquote(value_var) in unquote(field_ast)))
      }
    ]
  end
end
