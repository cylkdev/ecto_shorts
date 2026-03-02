defmodule EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.LowerUpper do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    lower_upper_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
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
        head: quote(do: {:not, {:==, {:lower, unquote(value_var)}}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:lower, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:==, {:upper, unquote(value_var)}}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:upper, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, {:lower, unquote(value_var)}}}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:lower, unquote(value_var)})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:!=, {:upper, unquote(value_var)}}}),
        body:
          quote do
            apply_dynamic_expr(unquote(binding_head_ast), unquote(key_var), {:upper, unquote(value_var)})
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:not, {:lower, unquote(value_var)}}),
        body:
          AST.dynamic_ast(
            binding_body_asts,
            quote do
              fragment(
                "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
                unquote(field_ast),
                ^unquote(value_var)
              )
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
              fragment(
                "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
                unquote(field_ast),
                ^unquote(value_var)
              )
            end
          )
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
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:lower, unquote(value_var)}}
            )
          end
      },
      %ClauseSpec{
        binding_head: binding_head_ast,
        key: key_var,
        head: quote(do: {:!=, {:upper, unquote(value_var)}}),
        body:
          quote do
            apply_dynamic_expr(
              unquote(binding_head_ast),
              unquote(key_var),
              {:not, {:upper, unquote(value_var)}}
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
              fragment(
                "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
                unquote(field_ast),
                ^unquote(value_var)
              )
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
              fragment(
                "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
                unquote(field_ast),
                ^unquote(value_var)
              )
            end
          )
      }
    ]
  end
end
