defmodule EctoShorts.Dynamics.Postgres.ScalarExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @equality_directives [
    :==,
    :eq,
    :!=,
    :ne
  ]

  @comparison_directives @equality_directives ++
                           [
                             :>,
                             :>=,
                             :<,
                             :<=,
                             :gt,
                             :gte,
                             :lt,
                             :lte
                           ]

  @membership_directives [
    :in
  ]

  @string_transform_directives [
    :lower,
    :upper
  ]

  @string_directives [
    :like,
    :ilike
  ]

  @directives [:membership, :comparison, :string_transform, :string]

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def directives, do: @directives

  def directives(:membership), do: @membership_directives
  def directives(:equality), do: @equality_directives
  def directives(:comparison), do: @comparison_directives
  def directives(:string_transform), do: @string_transform_directives
  def directives(:string), do: @string_directives

  @impl true
  def specs_for(directive, binding_selector_ast, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    negated_var = Macro.var(:negated, context)
    value_var = Macro.var(:value, context)

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: [negated_var, value_var],
        body: quote_body(directive, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context)
      }
    ]
  end

  @doc false
  def quote_body(:comparison, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context) do
    conditions =
      binding_selector_ast
      |> comparison_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    case_clause_ast(negated_var, value_var, conditions)
  end

  def quote_body(:membership, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context) do
    conditions =
      binding_selector_ast
      |> membership_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    case_clause_ast(negated_var, value_var, conditions)
  end

  def quote_body(:string_transform, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context) do
    conditions =
      binding_selector_ast
      |> string_transform_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    case_clause_ast(negated_var, value_var, conditions)
  end

  def quote_body(:string, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context) do
    conditions =
      binding_selector_ast
      |> string_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    case_clause_ast(negated_var, value_var, conditions)
  end

  defp case_clause_ast(negated_var, value_var, conditions) do
    quote do
      term =
        case unquote(negated_var) do
          :not -> {:not, unquote(value_var)}
          _ -> unquote(value_var)
        end

      case term, do: unquote(conditions)
    end
  end

  defp comparison_conditions({bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    Enum.flat_map(@comparison_directives, fn
      op when op in [:==, :eq] ->
        [
          quote do
            {:not, {unquote(op), nil}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr(op, q_var, {key_var, nil}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), nil} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr(op, q_var, {key_var, nil}),
                  context
                )
              )
          end,
          quote do
            {:not, {unquote(op), unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr(op, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), unquote(value_var)} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr(op, q_var, {key_var, value_var}),
                  context
                )
              )
          end
        ]

      op when op in [:!=, :ne] ->
        [
          quote do
            {:not, {unquote(op), nil}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr(op, q_var, {key_var, nil}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), nil} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr(op, q_var, {key_var, nil}),
                  context
                )
              )
          end,
          quote do
            {:not, {unquote(op), unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr(op, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), unquote(value_var)} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr(op, q_var, {key_var, value_var}),
                  context
                )
              )
          end
        ]

      op ->
        [
          quote do
            {:not, {unquote(op), unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr(op, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), unquote(value_var)} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr(op, q_var, {key_var, value_var}),
                  context
                )
              )
          end
        ]
    end)
  end

  defp membership_conditions({bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    [
      quote do
        {:not, {:in, unquote(value_var)}} ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_negated_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:in, unquote(value_var)} ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:not, {:==, unquote(value_var)}} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_negated_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:not, {:eq, unquote(value_var)}} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_negated_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:==, unquote(value_var)} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:eq, unquote(value_var)} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:not, {:!=, unquote(value_var)}} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:not, {:ne, unquote(value_var)}} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:!=, unquote(value_var)} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_negated_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end,
      quote do
        {:ne, unquote(value_var)} when is_list(unquote(value_var)) ->
          unquote(
            Helpers.dyn_expr(
              {bind_op, bind_to_var},
              q_var,
              quote_negated_expr(:in, q_var, {key_var, value_var}),
              context
            )
          )
      end
    ]
  end

  defp string_transform_conditions({bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    Enum.flat_map(@comparison_directives, fn
      op when op in [:==, :eq] ->
        [
          quote do
            {:not, {unquote(op), {:lower, unquote(value_var)}}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr({op, :lower}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), {:lower, unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr({op, :lower}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {:not, {unquote(op), {:upper, unquote(value_var)}}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr({op, :upper}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), {:upper, unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr({op, :upper}, q_var, {key_var, value_var}),
                  context
                )
              )
          end
        ]

      op when op in [:!=, :ne] ->
        [
          quote do
            {:not, {unquote(op), {:lower, unquote(value_var)}}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr({op, :lower}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), {:lower, unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr({op, :lower}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {:not, {unquote(op), {:upper, unquote(value_var)}}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr({op, :upper}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), {:upper, unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr({op, :upper}, q_var, {key_var, value_var}),
                  context
                )
              )
          end
        ]

      op ->
        [
          quote do
            {:not, {unquote(op), {:lower, unquote(value_var)}}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr({op, :lower}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), {:lower, unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr({op, :lower}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {:not, {unquote(op), {:upper, unquote(value_var)}}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_negated_expr({op, :upper}, q_var, {key_var, value_var}),
                  context
                )
              )
          end,
          quote do
            {unquote(op), {:upper, unquote(value_var)}} ->
              unquote(
                Helpers.dyn_expr(
                  {bind_op, bind_to_var},
                  q_var,
                  quote_expr({op, :upper}, q_var, {key_var, value_var}),
                  context
                )
              )
          end
        ]
    end)
  end

  defp string_conditions({bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    Enum.flat_map(@string_directives, fn string_op ->
      [
        quote do
          {:not, {unquote(string_op), unquote(value_var)}} when is_list(unquote(value_var)) ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_negated_expr({string_op, :any}, q_var, {key_var, value_var}),
                context
              )
            )
        end,
        quote do
          {unquote(string_op), unquote(value_var)} when is_list(unquote(value_var)) ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_expr({string_op, :any}, q_var, {key_var, value_var}),
                context
              )
            )
        end,
        quote do
          {:not, {unquote(string_op), unquote(value_var)}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_negated_expr(string_op, q_var, {key_var, value_var}),
                context
              )
            )
        end,
        quote do
          {unquote(string_op), unquote(value_var)} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_expr(string_op, q_var, {key_var, value_var}),
                context
              )
            )
        end
      ]
    end)
  end

  @doc false
  def quote_negated_expr({op, meta}, q_var, {key_var, value_var}) when op in [:==, :eq] do
    quote_expr({:!=, meta}, q_var, {key_var, value_var})
  end

  def quote_negated_expr({op, meta}, q_var, {key_var, value_var}) when op in [:!=, :ne] do
    quote_expr({:==, meta}, q_var, {key_var, value_var})
  end

  def quote_negated_expr(op, q_var, {key_var, value_var}) when op in [:==, :eq] do
    quote_expr(:!=, q_var, {key_var, value_var})
  end

  def quote_negated_expr(op, q_var, {key_var, value_var}) when op in [:!=, :ne] do
    quote_expr(:==, q_var, {key_var, value_var})
  end

  def quote_negated_expr({op, meta}, q_var, {key_var, value_var}) do
    {op, meta}
    |> quote_expr(q_var, {key_var, value_var})
    |> Helpers.negated_expr()
  end

  def quote_negated_expr(op, q_var, {key_var, value_var}) do
    op
    |> quote_expr(q_var, {key_var, value_var})
    |> Helpers.negated_expr()
  end

  @doc false
  def quote_expr(op, q_var, {key_var, nil}) when op in [:==, :eq] do
    quote do
      is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def quote_expr(op, q_var, {key_var, nil}) when op in [:!=, :ne] do
    quote do
      not is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def quote_expr({op, meta}, q_var, {key_var, value_var})
      when op in @comparison_directives and meta in [:lower, :upper] do
    content =
      if meta === :lower do
        "lower(?)"
      else
        "upper(?)"
      end

    fragment_expr =
      quote do
        fragment(unquote(content), field(unquote(q_var), ^unquote(key_var)))
      end

    quote do
      unquote(Helpers.special_form_ast(fragment_expr, op, Helpers.pinned_ast(value_var)))
    end
  end

  def quote_expr({op, :any}, q_var, {key_var, value_var}) when op in @string_directives do
    content =
      if op === :like do
        "? LIKE ANY(?)"
      else
        "? ILIKE ANY(?)"
      end

    patterns_expr =
      quote do
        Enum.map(unquote(value_var), fn value -> "%#{value}%" end)
      end

    quote do
      fragment(
        unquote(content),
        field(unquote(q_var), ^unquote(key_var)),
        ^unquote(patterns_expr)
      )
    end
  end

  def quote_expr(:like, q_var, {key_var, value_var}) do
    quote do
      like(field(unquote(q_var), ^unquote(key_var)), ^"%#{unquote(value_var)}%")
    end
  end

  def quote_expr(:ilike, q_var, {key_var, value_var}) do
    quote do
      ilike(field(unquote(q_var), ^unquote(key_var)), ^"%#{unquote(value_var)}%")
    end
  end

  def quote_expr(op, q_var, {key_var, value_var}) do
    field_expr =
      quote do
        field(unquote(q_var), ^unquote(key_var))
      end

    quote do
      unquote(Helpers.special_form_ast(field_expr, op, Helpers.pinned_ast(value_var)))
    end
  end
end
