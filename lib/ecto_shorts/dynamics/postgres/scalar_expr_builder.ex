defmodule EctoShorts.Dynamics.Postgres.ScalarExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @equality_operators [
    :==,
    :eq,
    :!=,
    :ne
  ]

  @comparison_operators [:>, :>=, :<, :<=, :gt, :gte, :lt, :lte | @equality_operators]

  @aggregate_helpers [
    :avg,
    :count,
    :max,
    :min,
    :sum
  ]

  @quantifier_operators [
    :all,
    :any
  ]

  @datetime_interval_literals [
    "year",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond"
  ]

  @membership_operators [
    :in
  ]

  @string_transform_operators [
    :lower,
    :upper
  ]

  @string_operators [
    :like,
    :ilike
  ]

  @operators [:membership, :comparison, :string_transform, :string]

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def operators, do: @operators

  def operators(:membership), do: @membership_operators
  def operators(:equality), do: @equality_operators
  def operators(:comparison), do: @comparison_operators
  def operators(:string_transform), do: @string_transform_operators
  def operators(:string), do: @string_operators

  @impl true
  def specs_for(operator, binding_selector_ast, q_var, opts) do
    context = opts[:context]

    key_var = Macro.var(:key, context)
    negated_var = Macro.var(:negated, context)
    value_var = Macro.var(:value, context)

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: [negated_var, value_var],
        body: quote_body(operator, binding_selector_ast, {q_var, key_var, negated_var, value_var}, context)
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
    quantified_value_var = Macro.var(:quantified_value, context)
    wrapped_value_var = Macro.var(:wrapped_value, context)
    arithmetic_op_var = Macro.var(:arithmetic_op, context)
    datetime_count_var = Macro.var(:datetime_count, context)
    field_name_var = Macro.var(:field_name, context)
    scalar_value_var = Macro.var(:scalar_value, context)

    plain_conditions =
      Enum.flat_map(@comparison_operators, fn
        op when op in [:==, :eq] ->
          [
            quote do
              {:not, {unquote(op), {:all, unquote(quantified_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:all, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:all, unquote(quantified_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:all, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), {:any, unquote(quantified_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:any, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:any, unquote(quantified_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:any, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
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
              {
                :not,
                {
                  unquote(op),
                  {
                    :value,
                    {
                      unquote(arithmetic_op_var),
                      [
                        {:field, unquote(field_name_var)},
                        {:value, unquote(scalar_value_var)}
                      ]
                    }
                  }
                }
              }
              when unquote(arithmetic_op_var) in [:+, :-, :*, :/] ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var,
                       {:value, {arithmetic_op_var, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value,
                {unquote(arithmetic_op_var),
                 [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}}
              when unquote(arithmetic_op_var) in [:+, :-, :*, :/] ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var,
                       {:value, {arithmetic_op_var, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), {:value, unquote(wrapped_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(op, q_var, {key_var, {:value, wrapped_value_var}}),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:value, unquote(wrapped_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(op, q_var, {key_var, {:value, wrapped_value_var}}),
                    context
                  )
                )
            end,
            datetime_conditions(
              op,
              {bind_op, bind_to_var},
              q_var,
              key_var,
              field_name_var,
              datetime_count_var,
              context
            ),
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
              {:not, {unquote(op), {:all, unquote(quantified_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:all, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:all, unquote(quantified_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:all, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), {:any, unquote(quantified_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:any, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:any, unquote(quantified_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:any, quantified_value_var}}
                    ),
                    context
                  )
                )
            end,
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
              {:not,
               {unquote(op),
                {:value, {:+, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:+, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:+, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:+, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not,
               {unquote(op),
                {:value, {:-, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:-, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:-, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:-, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not,
               {unquote(op),
                {:value, {:*, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:*, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:*, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:*, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not,
               {unquote(op),
                {:value, {:/, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:/, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:/, [{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}]}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:/, [{:field, field_name_var}, {:value, scalar_value_var}]}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), {:value, unquote(wrapped_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(op, q_var, {key_var, {:value, wrapped_value_var}}),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:value, unquote(wrapped_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(op, q_var, {key_var, {:value, wrapped_value_var}}),
                    context
                  )
                )
            end,
            datetime_conditions(
              op,
              {bind_op, bind_to_var},
              q_var,
              key_var,
              field_name_var,
              datetime_count_var,
              context
            ),
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
              {:not,
               {unquote(op),
                {:value, {:+, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:+, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:+, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:+, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not,
               {unquote(op),
                {:value, {:-, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:-, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:-, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:-, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not,
               {unquote(op),
                {:value, {:*, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:*, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:*, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:*, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not,
               {unquote(op),
                {:value, {:/, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:/, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {unquote(op),
               {:value, {:/, {{:field, unquote(field_name_var)}, {:value, unquote(scalar_value_var)}}}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(
                      op,
                      q_var,
                      {key_var, {:value, {:/, {{:field, field_name_var}, {:value, scalar_value_var}}}}}
                    ),
                    context
                  )
                )
            end,
            quote do
              {:not, {unquote(op), {:value, unquote(wrapped_value_var)}}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_negated_expr(op, q_var, {key_var, {:value, wrapped_value_var}}),
                    context
                  )
                )
            end,
            quote do
              {unquote(op), {:value, unquote(wrapped_value_var)}} ->
                unquote(
                  Helpers.dyn_expr(
                    {bind_op, bind_to_var},
                    q_var,
                    quote_expr(op, q_var, {key_var, {:value, wrapped_value_var}}),
                    context
                  )
                )
            end,
            datetime_conditions(
              op,
              {bind_op, bind_to_var},
              q_var,
              key_var,
              field_name_var,
              datetime_count_var,
              context
            ),
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

    aggregate_conditions =
      Enum.flat_map(@aggregate_helpers, fn helper ->
        Enum.flat_map(@comparison_operators, fn
          op when op in [:==, :eq] ->
            [
              quote do
                {:not, {unquote(helper), {unquote(op), nil}}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_negated_expr({op, helper}, q_var, {key_var, nil}),
                      context
                    )
                  )
              end,
              quote do
                {unquote(helper), {unquote(op), nil}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_expr({op, helper}, q_var, {key_var, nil}),
                      context
                    )
                  )
              end,
              quote do
                {:not, {unquote(helper), {unquote(op), unquote(value_var)}}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_negated_expr({op, helper}, q_var, {key_var, value_var}),
                      context
                    )
                  )
              end,
              quote do
                {unquote(helper), {unquote(op), unquote(value_var)}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_expr({op, helper}, q_var, {key_var, value_var}),
                      context
                    )
                  )
              end
            ]

          op when op in [:!=, :ne] ->
            [
              quote do
                {:not, {unquote(helper), {unquote(op), nil}}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_negated_expr({op, helper}, q_var, {key_var, nil}),
                      context
                    )
                  )
              end,
              quote do
                {unquote(helper), {unquote(op), nil}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_expr({op, helper}, q_var, {key_var, nil}),
                      context
                    )
                  )
              end,
              quote do
                {:not, {unquote(helper), {unquote(op), unquote(value_var)}}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_negated_expr({op, helper}, q_var, {key_var, value_var}),
                      context
                    )
                  )
              end,
              quote do
                {unquote(helper), {unquote(op), unquote(value_var)}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_expr({op, helper}, q_var, {key_var, value_var}),
                      context
                    )
                  )
              end
            ]

          op ->
            [
              quote do
                {:not, {unquote(helper), {unquote(op), unquote(value_var)}}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_negated_expr({op, helper}, q_var, {key_var, value_var}),
                      context
                    )
                  )
              end,
              quote do
                {unquote(helper), {unquote(op), unquote(value_var)}} ->
                  unquote(
                    Helpers.dyn_expr(
                      {bind_op, bind_to_var},
                      q_var,
                      quote_expr({op, helper}, q_var, {key_var, value_var}),
                      context
                    )
                  )
              end
            ]
        end)
      end)

    plain_conditions ++ aggregate_conditions
  end

  defp datetime_conditions(
         op,
         {bind_op, bind_to_var},
         q_var,
         key_var,
         field_name_var,
         datetime_count_var,
         context
       ) do
    Enum.flat_map(@datetime_interval_literals, fn interval ->
      [
        quote do
          {:not,
           {unquote(op),
            {:datetime,
             {:add,
              [
                field: unquote(field_name_var),
                count: unquote(datetime_count_var),
                interval: unquote(interval)
              ]}}}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_negated_expr(
                  op,
                  q_var,
                  {key_var,
                   {:datetime, {:add, [field: field_name_var, count: datetime_count_var, interval: interval]}}}
                ),
                context
              )
            )
        end,
        quote do
          {unquote(op),
           {:datetime,
            {:add,
             [field: unquote(field_name_var), count: unquote(datetime_count_var), interval: unquote(interval)]}}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_expr(
                  op,
                  q_var,
                  {key_var,
                   {:datetime, {:add, [field: field_name_var, count: datetime_count_var, interval: interval]}}}
                ),
                context
              )
            )
        end,
        quote do
          {:not,
           {unquote(op),
            {:datetime, {:ago, [count: unquote(datetime_count_var), interval: unquote(interval)]}}}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_negated_expr(
                  op,
                  q_var,
                  {key_var, {:datetime, {:ago, [count: datetime_count_var, interval: interval]}}}
                ),
                context
              )
            )
        end,
        quote do
          {unquote(op),
           {:datetime, {:ago, [count: unquote(datetime_count_var), interval: unquote(interval)]}}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_expr(
                  op,
                  q_var,
                  {key_var, {:datetime, {:ago, [count: datetime_count_var, interval: interval]}}}
                ),
                context
              )
            )
        end,
        quote do
          {:not,
           {unquote(op),
            {:datetime, {:from_now, [count: unquote(datetime_count_var), interval: unquote(interval)]}}}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_negated_expr(
                  op,
                  q_var,
                  {key_var, {:datetime, {:from_now, [count: datetime_count_var, interval: interval]}}}
                ),
                context
              )
            )
        end,
        quote do
          {unquote(op),
           {:datetime, {:from_now, [count: unquote(datetime_count_var), interval: unquote(interval)]}}} ->
            unquote(
              Helpers.dyn_expr(
                {bind_op, bind_to_var},
                q_var,
                quote_expr(
                  op,
                  q_var,
                  {key_var, {:datetime, {:from_now, [count: datetime_count_var, interval: interval]}}}
                ),
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
    Enum.flat_map(@comparison_operators, fn
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
    Enum.flat_map(@string_operators, fn string_op ->
      [
        quote do
          {:not, {unquote(string_op), unquote(value_var)}} when is_list(unquote(value_var)) ->
            unquote(
              quote_negated_string_list_expr(
                {bind_op, bind_to_var},
                q_var,
                string_op,
                key_var,
                value_var,
                context
              )
            )
        end,
        quote do
          {unquote(string_op), unquote(value_var)} when is_list(unquote(value_var)) ->
            unquote(
              quote_string_list_expr(
                {bind_op, bind_to_var},
                q_var,
                string_op,
                key_var,
                value_var,
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

  defp quote_string_list_expr(binding_selector_ast, q_var, string_op, key_var, value_var, context) do
    Helpers.dyn_expr(
      binding_selector_ast,
      q_var,
      quote_expr({string_op, :any}, q_var, {key_var, value_var}),
      context
    )
  end

  defp quote_negated_string_list_expr(
         binding_selector_ast,
         q_var,
         string_op,
         key_var,
         value_var,
         context
       ) do
    Helpers.dyn_expr(
      binding_selector_ast,
      q_var,
      quote_negated_expr({string_op, :any}, q_var, {key_var, value_var}),
      context
    )
  end

  @doc false
  def quote_negated_expr(op, q_var, {key_var, {quantifier, value_var}})
      when op in @equality_operators and quantifier in @quantifier_operators do
    op
    |> quote_expr(q_var, {key_var, {quantifier, value_var}})
    |> Helpers.negated_expr()
  end

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

  def quote_expr(op, q_var, {key_var, {quantifier, value_var}})
      when op in @equality_operators and quantifier in @quantifier_operators do
    field_expr =
      quote do
        field(unquote(q_var), ^unquote(key_var))
      end

    quote do
      unquote(Helpers.special_form_ast(field_expr, op, quantified_expr(quantifier, value_var)))
    end
  end

  def quote_expr({op, meta}, q_var, {key_var, value_var})
      when op in @comparison_operators and meta in @aggregate_helpers do
    quote do
      unquote(
        Helpers.special_form_ast(
          aggregate_expr(meta, q_var, key_var),
          op,
          Helpers.pinned_ast(value_var)
        )
      )
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

  def quote_expr(op, q_var, {key_var, {:datetime, value_var}}) do
    field_expr =
      quote do
        field(unquote(q_var), ^unquote(key_var))
      end

    quote do
      unquote(Helpers.special_form_ast(field_expr, op, value_expr_ast(q_var, {:datetime, value_var})))
    end
  end

  def quote_expr(op, q_var, {key_var, {:value, value_var}}) do
    field_expr =
      quote do
        field(unquote(q_var), ^unquote(key_var))
      end

    quote do
      unquote(Helpers.special_form_ast(field_expr, op, value_expr_ast(q_var, value_var)))
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

  defp quantified_expr(:all, value_var) do
    quote do
      all(unquote(value_var))
    end
  end

  defp quantified_expr(:any, value_var) do
    quote do
      any(unquote(value_var))
    end
  end

  defp aggregate_expr(:avg, q_var, key_var) do
    quote do
      avg(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  defp aggregate_expr(:count, q_var, key_var) do
    quote do
      count(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  defp aggregate_expr(:max, q_var, key_var) do
    quote do
      max(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  defp aggregate_expr(:min, q_var, key_var) do
    quote do
      min(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  defp aggregate_expr(:sum, q_var, key_var) do
    quote do
      sum(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  defp value_expr_ast(q_var, {:field, field_name}) do
    quote do
      field(unquote(q_var), ^unquote(field_name))
    end
  end

  defp value_expr_ast(_q_var, {:value, value}) do
    Helpers.pinned_ast(value)
  end

  defp value_expr_ast(q_var, {:datetime, value}) do
    datetime_value_expr_ast(q_var, value)
  end

  defp value_expr_ast(q_var, {op, {left, right}}) when op in [:+, :-, :*, :/] do
    left_ast = value_expr_ast(q_var, left)
    right_ast = value_expr_ast(q_var, right)

    arithmetic_expr_ast(op, left_ast, right_ast)
  end

  defp value_expr_ast(_q_var, value) do
    Helpers.pinned_ast(value)
  end

  defp arithmetic_expr_ast(:+, left_ast, right_ast) do
    quote do
      unquote(left_ast) + unquote(right_ast)
    end
  end

  defp arithmetic_expr_ast(:-, left_ast, right_ast) do
    quote do
      unquote(left_ast) - unquote(right_ast)
    end
  end

  defp arithmetic_expr_ast(:*, left_ast, right_ast) do
    quote do
      unquote(left_ast) * unquote(right_ast)
    end
  end

  defp arithmetic_expr_ast(:/, left_ast, right_ast) do
    quote do
      unquote(left_ast) / unquote(right_ast)
    end
  end

  defp datetime_value_expr_ast(q_var, {:add, params}) do
    field_name = Keyword.fetch!(params, :field)
    count = Keyword.fetch!(params, :count)
    interval = Keyword.fetch!(params, :interval)

    quote do
      datetime_add(field(unquote(q_var), ^unquote(field_name)), ^unquote(count), unquote(interval))
    end
  end

  defp datetime_value_expr_ast(_q_var, {:ago, params}) do
    count = Keyword.fetch!(params, :count)
    interval = Keyword.fetch!(params, :interval)

    quote do
      ago(^unquote(count), unquote(interval))
    end
  end

  defp datetime_value_expr_ast(_q_var, {:from_now, params}) do
    count = Keyword.fetch!(params, :count)
    interval = Keyword.fetch!(params, :interval)

    quote do
      from_now(^unquote(count), unquote(interval))
    end
  end
end
