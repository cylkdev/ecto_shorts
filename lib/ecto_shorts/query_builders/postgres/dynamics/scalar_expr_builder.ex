defmodule EctoShorts.QueryBuilder.Dynamics.ScalarExprBuilder do
  @moduledoc false

  alias EctoShorts.QueryBuilder.BindingHelpers
  alias EctoShorts.QueryBuilder.Dynamics.ScalarExprBuilder

  defmacro define_scalar_exprs(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    asts = ScalarExprBuilder.scalar_exprs_ast(target_binding_var, binding_patterns)

    quote do
      (unquote_splicing(asts))
    end
  end

  def scalar_exprs_ast(target_binding_var, binding_patterns) do
    for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
      quote do
        def dynamic_field_expr(unquote(quoted_binding_head), key, {op, nil}) do
          case op do
            :== ->
              Ecto.Query.dynamic(
                [unquote_splicing(quoted_binding_body)],
                is_nil(field(unquote(target_binding_var), ^key))
              )

            :!= ->
              Ecto.Query.dynamic(
                [unquote_splicing(quoted_binding_body)],
                not is_nil(field(unquote(target_binding_var), ^key))
              )

            _ ->
              raise ArgumentError,
                message:
                  "Expected the operator to be one of [:==, :!=] for nil comparison, got: #{inspect(op)}"
          end
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:lower, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (fragment("lower(?)", field(unquote(target_binding_var), ^key)) == ^value)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:upper, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (fragment("upper(?)", field(unquote(target_binding_var), ^key)) == ^value)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, {:lower, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:lower, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, {:upper, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:upper, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, {:lower, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("lower(?)", field(unquote(target_binding_var), ^key)) != ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, {:upper, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("upper(?)", field(unquote(target_binding_var), ^key)) != ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:lower, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("lower(?)", field(unquote(target_binding_var), ^key)) == ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:upper, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("upper(?)", field(unquote(target_binding_var), ^key)) == ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:like, values}})
            when is_list(values) do
          patterns = Enum.map(values, &"%#{&1}%")

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? LIKE ANY(?)", field(unquote(target_binding_var), ^key), ^patterns)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:like, value}}) do
          search_query = "%#{value}%"

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not like(field(unquote(target_binding_var), ^key), ^search_query)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:ilike, values}})
            when is_list(values) do
          patterns = Enum.map(values, &"%#{&1}%")

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? ILIKE ANY(?)", field(unquote(target_binding_var), ^key), ^patterns)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:ilike, value}}) do
          search_query = "%#{value}%"

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not ilike(field(unquote(target_binding_var), ^key), ^search_query)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:==, values}})
            when is_list(values) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:in, values}})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:!=, values}})
            when is_list(values) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:in, values})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, values})
            when is_list(values) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:in, values})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, values})
            when is_list(values) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:in, values}})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:==, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:!=, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:==, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:in, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) not in ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:>, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^key) > ^value)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:>=, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^key) >= ^value)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:<, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^key) < ^value)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:<=, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^key) <= ^value)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:like, values})
            when is_list(values) do
          patterns = Enum.map(values, &"%#{&1}%")

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? LIKE ANY(?)", field(unquote(target_binding_var), ^key), ^patterns)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:like, value}) do
          search_query = "%#{value}%"

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            like(field(unquote(target_binding_var), ^key), ^search_query)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:ilike, values})
            when is_list(values) do
          patterns = Enum.map(values, &"%#{&1}%")

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? ILIKE ANY(?)", field(unquote(target_binding_var), ^key), ^patterns)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:ilike, value}) do
          search_query = "%#{value}%"

          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            ilike(field(unquote(target_binding_var), ^key), ^search_query)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) == ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) != ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:in, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) in ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:>, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) > ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:>=, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) >= ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:<, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) < ^value
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:<=, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) <= ^value
          )
        end
      end
    end
  end
end
