defmodule EctoShorts.QueryBuilders.Dynamics.ArrayExprBuilder do
  @moduledoc false

  alias EctoShorts.QueryBuilders.BindingHelpers
  alias EctoShorts.QueryBuilders.Dynamics.ArrayExprBuilder

  defmacro define_nil_comparisons(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    asts = ArrayExprBuilder.nil_comparisons_ast(target_binding_var, binding_patterns)

    quote do
      (unquote_splicing(asts))
    end
  end

  defmacro define_case_transforms(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    asts = ArrayExprBuilder.case_transforms_ast(target_binding_var, binding_patterns)

    quote do
      (unquote_splicing(asts))
    end
  end

  defmacro define_like_ilikes(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    asts = ArrayExprBuilder.like_ilike_ast(target_binding_var, binding_patterns)

    quote do
      (unquote_splicing(asts))
    end
  end

  defmacro define_base_ops(context_ast \\ nil, opts_ast \\ []) do
    context = Macro.expand(context_ast, __CALLER__)
    opts = Macro.expand(opts_ast, __CALLER__)

    {target_binding_var, binding_patterns} =
      BindingHelpers.query_var_and_binding_heads(context, opts)

    asts = ArrayExprBuilder.base_ops_ast(target_binding_var, binding_patterns)

    quote do
      (unquote_splicing(asts))
    end
  end

  def nil_comparisons_ast(target_binding_var, binding_patterns) do
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
      end
    end
  end

  def case_transforms_ast(target_binding_var, binding_patterns) do
    for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
      quote do
        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:lower, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              field(unquote(target_binding_var), ^key),
              ^value
            )
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:upper, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              field(unquote(target_binding_var), ^key),
              ^value
            )
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, {:lower, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:lower, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, {:upper, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:upper, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, {:lower, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:lower, value}})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, {:upper, value}}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:upper, value}})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:lower, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              field(unquote(target_binding_var), ^key),
              ^value
            )
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:upper, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              field(unquote(target_binding_var), ^key),
              ^value
            )
          )
        end
      end
    end
  end

  def like_ilike_ast(target_binding_var, binding_patterns) do
    blocks =
      for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
        quote do
          def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:ilike, value}}) do
            patterns = normalize_patterns(value)

            Ecto.Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                """
                NOT EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE t ILIKE ANY (?)
                )
                """,
                field(unquote(target_binding_var), ^key),
                ^patterns
              )
            )
          end

          def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:like, value}}) do
            patterns = normalize_patterns(value)

            Ecto.Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                """
                NOT EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE t LIKE ANY (?)
                )
                """,
                field(unquote(target_binding_var), ^key),
                ^patterns
              )
            )
          end

          def dynamic_field_expr(unquote(quoted_binding_head), key, {:ilike, value}) do
            patterns = normalize_patterns(value)

            Ecto.Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                """
                EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE t ILIKE ANY (?)
                )
                """,
                field(unquote(target_binding_var), ^key),
                ^patterns
              )
            )
          end

          def dynamic_field_expr(unquote(quoted_binding_head), key, {:like, value}) do
            patterns = normalize_patterns(value)

            Ecto.Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              fragment(
                """
                EXISTS (
                  SELECT 1
                  FROM unnest(?) AS t
                  WHERE t LIKE ANY (?)
                )
                """,
                field(unquote(target_binding_var), ^key),
                ^patterns
              )
            )
          end
        end
      end

    blocks ++
      [
        quote do
          defp normalize_patterns(value) when is_list(value),
            do: Enum.map(value, &"%#{&1}%")

          defp normalize_patterns(value),
            do: ["%#{value}%"]
        end
      ]
  end

  def base_ops_ast(target_binding_var, binding_patterns) do
    for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
      quote do
        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:==, values}})
            when is_list(values) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, values})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:!=, values}})
            when is_list(values) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:==, values})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:in, values}})
            when is_list(values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment(
              "? && ?",
              field(unquote(target_binding_var), ^key),
              ^values
            )
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:>, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? < ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:>=, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? <= ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:<, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? > ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:<=, value}}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? >= ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:in, {:all, values}}})
            when is_list(values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment("? @> ?", field(unquote(target_binding_var), ^key), ^values)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, values})
            when is_list(values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) == ^values
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, values})
            when is_list(values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^key) != ^values
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:in, values})
            when is_list(values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? && ?", field(unquote(target_binding_var), ^key), ^values)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:in, {:all, values}})
            when is_list(values) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? @> ?", field(unquote(target_binding_var), ^key), ^values)
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:>, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? < ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:>=, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? <= ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:<, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? > ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:<=, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("? >= ANY(?)", ^value, field(unquote(target_binding_var), ^key))
          )
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:==, value}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:in, value})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:!=, value}) do
          dynamic_field_expr(unquote(quoted_binding_head), key, {:not, {:in, value}})
        end

        def dynamic_field_expr(unquote(quoted_binding_head), key, {:in, value}) do
          Ecto.Query.dynamic(
            [unquote_splicing(quoted_binding_body)],
            ^value in field(unquote(target_binding_var), ^key)
          )
        end
      end
    end
  end
end
