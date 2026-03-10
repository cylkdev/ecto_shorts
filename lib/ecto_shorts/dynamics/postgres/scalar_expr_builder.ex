defmodule EctoShorts.Dynamics.Postgres.ScalarExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @comparison_operators [
    :==,
    :eq,
    :!=,
    :ne,
    :>,
    :>=,
    :<,
    :<=,
    :gt,
    :gte,
    :lt,
    :lte
  ]

  @string_operators [
    :like,
    :ilike
  ]

  @keys [:comparison, :membership, :string]

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def keys, do: @keys

  @impl true
  def specs_for(kind, {bind_op, bind_to_var}, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    value_var = Macro.var(:value, context)

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: value_var,
        body: quote_body(kind, {bind_op, bind_to_var}, q_var, key_var, value_var, context)
      }
    ]
  end

  @doc false
  def quote_body(:comparison, {bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    conditions =
      {bind_op, bind_to_var}
      |> comparison_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    {:case, [], [value_var, [do: conditions]]}
  end

  def quote_body(:membership, {bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    conditions =
      {bind_op, bind_to_var}
      |> membership_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    {:case, [], [value_var, [do: conditions]]}
  end

  def quote_body(:string, {bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    conditions =
      {bind_op, bind_to_var}
      |> string_conditions(q_var, key_var, value_var, context)
      |> List.flatten()

    {:case, [], [value_var, [do: conditions]]}
  end

  defp comparison_conditions({bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    Enum.flat_map(@comparison_operators, fn
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

  defp string_conditions({bind_op, bind_to_var}, q_var, key_var, value_var, context) do
    Enum.flat_map(@string_operators, fn string_op ->
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
      when op in @comparison_operators and meta in [:lower, :upper] do
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

  def quote_expr({op, :any}, q_var, {key_var, value_var}) when op in @string_operators do
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
