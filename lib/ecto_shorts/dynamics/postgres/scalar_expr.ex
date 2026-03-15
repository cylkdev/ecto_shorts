defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  import Ecto.Query

  alias EctoShorts.Compiler
  alias EctoShorts.Dynamics.Postgres.ScalarExprComparisonQuote

  @max_positional_bindings 10
  @aggregate_helpers [:avg, :count, :max, :min, :sum]
  @operators [:membership, :comparison, :string_transform, :string]
  @comparison_operators [:>, :>=, :<, :<=, :==, :!=]
  @equality_operators [:==, :!=]
  @string_operators [:like, :ilike]

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(@max_positional_bindings, __MODULE__)

  context = __MODULE__
  key_var = Macro.var(:key, context)
  negated_var = Macro.var(:negated, context)
  value_var = Macro.var(:value, context)

  def operators, do: @operators

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    def dynamic_expr(selected_binding = unquote(quoted_binding_head), key, negated, term, _opts) do
      {op, normalized_term} = normalize_term(term)

      case family_for(op, normalized_term) do
        :membership ->
          membership_expr(selected_binding, key, negated, {op, normalized_term})

        :string_transform ->
          string_transform_expr(selected_binding, key, negated, {op, normalized_term})

        :string ->
          string_expr(selected_binding, key, negated, {op, normalized_term})

        :comparison ->
          comparison_expr(selected_binding, key, negated, {op, normalized_term})
      end
    end

    defp membership_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      term =
        case unquote(negated_var) do
          :not -> {:not, unquote(value_var)}
          _ -> unquote(value_var)
        end

      case term do
        {:not, {:in, values}} when is_list(values) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            is_nil(field(unquote(target_binding_var), ^unquote(key_var))) or
              field(unquote(target_binding_var), ^unquote(key_var)) not in ^values
          )

        {:in, values} when is_list(values) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) in ^values
          )

        {:not, {:==, values}} when is_list(values) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            is_nil(field(unquote(target_binding_var), ^unquote(key_var))) or
              field(unquote(target_binding_var), ^unquote(key_var)) not in ^values
          )

        {:==, values} when is_list(values) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) in ^values
          )

        {:not, {:!=, values}} when is_list(values) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not is_nil(field(unquote(target_binding_var), ^unquote(key_var))) and
              field(unquote(target_binding_var), ^unquote(key_var)) in ^values
          )

        {:!=, values} when is_list(values) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            is_nil(field(unquote(target_binding_var), ^unquote(key_var))) or
              field(unquote(target_binding_var), ^unquote(key_var)) not in ^values
          )

        _ ->
          nil
      end
    end

    defp comparison_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      term =
        case unquote(negated_var) do
          :not -> {:not, unquote(value_var)}
          _ -> unquote(value_var)
        end

      case term do
        {:not, {:avg, {:>, aggregate_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (avg(field(unquote(target_binding_var), ^unquote(key_var))) > ^aggregate_value)
          )

        {:avg, {:>, aggregate_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            avg(field(unquote(target_binding_var), ^unquote(key_var))) > ^aggregate_value
          )

        {:not, {:count, {:>, aggregate_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (count(field(unquote(target_binding_var), ^unquote(key_var))) > ^aggregate_value)
          )

        {:count, {:>, aggregate_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            count(field(unquote(target_binding_var), ^unquote(key_var))) > ^aggregate_value
          )

        {:count, {:==, aggregate_value}} when not is_nil(aggregate_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            count(field(unquote(target_binding_var), ^unquote(key_var))) == ^aggregate_value
          )

        {:not, {:max, {:>=, aggregate_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (max(field(unquote(target_binding_var), ^unquote(key_var))) >= ^aggregate_value)
          )

        {:max, {:>=, aggregate_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            max(field(unquote(target_binding_var), ^unquote(key_var))) >= ^aggregate_value
          )

        {:not, {:min, {:<, aggregate_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (min(field(unquote(target_binding_var), ^unquote(key_var))) < ^aggregate_value)
          )

        {:min, {:<, aggregate_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            min(field(unquote(target_binding_var), ^unquote(key_var))) < ^aggregate_value
          )

        {:min, {:==, aggregate_value}} when not is_nil(aggregate_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            min(field(unquote(target_binding_var), ^unquote(key_var))) == ^aggregate_value
          )

        {:not, {:sum, {:>, aggregate_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (sum(field(unquote(target_binding_var), ^unquote(key_var))) > ^aggregate_value)
          )

        {:sum, {:>, aggregate_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            sum(field(unquote(target_binding_var), ^unquote(key_var))) > ^aggregate_value
          )

        {:sum, {:==, aggregate_value}} when not is_nil(aggregate_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            sum(field(unquote(target_binding_var), ^unquote(key_var))) == ^aggregate_value
          )

        {:sum, {:!=, aggregate_value}} when not is_nil(aggregate_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            sum(field(unquote(target_binding_var), ^unquote(key_var))) != ^aggregate_value
          )

        {:avg, {:!=, aggregate_value}} when not is_nil(aggregate_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            avg(field(unquote(target_binding_var), ^unquote(key_var))) != ^aggregate_value
          )

        {:avg, {:<=, aggregate_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            avg(field(unquote(target_binding_var), ^unquote(key_var))) <= ^aggregate_value
          )

        {:>, {:value, {:+, {{:field, arithmetic_field}, {:value, scalar_value}}}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) >
              field(unquote(target_binding_var), ^arithmetic_field) + ^scalar_value
          )

        {:==, {:date, {:ago, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("date(?)", field(unquote(target_binding_var), ^unquote(key_var))) ==
              fragment("date(?)", ago(^count, ^interval))
          )

        {:!=, {:date, {:from_now, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("date(?)", field(unquote(target_binding_var), ^unquote(key_var))) !=
              fragment("date(?)", from_now(^count, ^interval))
          )

        {:not, {:>, {:date, {:from_now, params}}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (
              fragment("date(?)", field(unquote(target_binding_var), ^unquote(key_var))) >
                fragment("date(?)", from_now(^count, ^interval))
            )
          )

        {:>=, {:date, {:add, params}}} ->
          field_name = Keyword.get(params, :field)
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("date(?)", field(unquote(target_binding_var), ^unquote(key_var))) >=
              fragment(
                "date(?)",
                datetime_add(field(unquote(target_binding_var), ^field_name), ^count, ^interval)
              )
          )

        {:>=, {:datetime, {:add, params}}} ->
          field_name = Keyword.get(params, :field)
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) >=
              datetime_add(field(unquote(target_binding_var), ^field_name), ^count, ^interval)
          )

        {:not, {:>=, {:datetime, {:add, params}}}} ->
          field_name = Keyword.get(params, :field)
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (
              field(unquote(target_binding_var), ^unquote(key_var)) >=
                datetime_add(field(unquote(target_binding_var), ^field_name), ^count, ^interval)
            )
          )

        {:>, {:datetime, {:ago, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) > ago(^count, ^interval)
          )

        {:>, {:datetime, {:from_now, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) > from_now(^count, ^interval)
          )

        {:not, {:<, {:datetime, {:ago, params}}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) < ago(^count, ^interval))
          )

        {:<, {:datetime, {:ago, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) < ago(^count, ^interval)
          )

        {:<=, {:datetime, {:from_now, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) <= from_now(^count, ^interval)
          )

        {:<, {:date, {:ago, params}}} ->
          count = Keyword.fetch!(params, :count)
          interval = Keyword.fetch!(params, :interval)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("date(?)", field(unquote(target_binding_var), ^unquote(key_var))) <
              fragment("date(?)", ago(^count, ^interval))
          )

        {:not, {:==, nil}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not is_nil(field(unquote(target_binding_var), ^unquote(key_var)))
          )

        {:==, nil} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            is_nil(field(unquote(target_binding_var), ^unquote(key_var)))
          )

        {:not, {:!=, nil}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            is_nil(field(unquote(target_binding_var), ^unquote(key_var)))
          )

        {:!=, nil} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not is_nil(field(unquote(target_binding_var), ^unquote(key_var)))
          )

        {:not, {:==, scalar_value}} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) != ^scalar_value
          )

        {:==, scalar_value} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) == ^scalar_value
          )

        {:not, {:!=, scalar_value}} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) == ^scalar_value
          )

        {:!=, scalar_value} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) != ^scalar_value
          )

        {:not, {:>, scalar_value}} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) > ^scalar_value)
          )

        {:>, scalar_value} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) > ^scalar_value
          )

        {:not, {:>=, scalar_value}} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) >= ^scalar_value)
          )

        {:>=, scalar_value} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) >= ^scalar_value
          )

        {:not, {:<, scalar_value}} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) < ^scalar_value)
          )

        {:<, scalar_value} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) < ^scalar_value
          )

        {:not, {:<=, scalar_value}} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) <= ^scalar_value)
          )

        {:<=, scalar_value} when not is_tuple(scalar_value) ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) <= ^scalar_value
          )

        {:not, {:==, {:all, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) == all(quantified_value))
          )

        {:==, {:all, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) == all(quantified_value)
          )

        {:not, {:==, {:any, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) == any(quantified_value))
          )

        {:==, {:any, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) == any(quantified_value)
          )

        {:not, {:!=, {:all, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) != all(quantified_value))
          )

        {:!=, {:all, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) != all(quantified_value)
          )

        {:not, {:!=, {:any, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) != any(quantified_value))
          )

        {:!=, {:any, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) != any(quantified_value)
          )

        {:not, {:>, {:all, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) > all(quantified_value))
          )

        {:>, {:all, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) > all(quantified_value)
          )

        {:not, {:>, {:any, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) > any(quantified_value))
          )

        {:>, {:any, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) > any(quantified_value)
          )

        {:not, {:>=, {:all, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) >= all(quantified_value))
          )

        {:>=, {:all, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) >= all(quantified_value)
          )

        {:not, {:>=, {:any, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) >= any(quantified_value))
          )

        {:>=, {:any, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) >= any(quantified_value)
          )

        {:not, {:<, {:all, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) < all(quantified_value))
          )

        {:<, {:all, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) < all(quantified_value)
          )

        {:not, {:<, {:any, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) < any(quantified_value))
          )

        {:<, {:any, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) < any(quantified_value)
          )

        {:not, {:<=, {:all, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) <= all(quantified_value))
          )

        {:<=, {:all, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) <= all(quantified_value)
          )

        {:not, {:<=, {:any, quantified_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not (field(unquote(target_binding_var), ^unquote(key_var)) <= any(quantified_value))
          )

        {:<=, {:any, quantified_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            field(unquote(target_binding_var), ^unquote(key_var)) <= any(quantified_value)
          )

        _ ->
          unquote(
            ScalarExprComparisonQuote.quote_body(
              quoted_binding_head,
              {target_binding_var, key_var, negated_var, value_var},
              context
            )
          )
      end
    end

    defp string_transform_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      term =
        case unquote(negated_var) do
          :not -> {:not, unquote(value_var)}
          _ -> unquote(value_var)
        end

      case term do
        {:not, {:==, {:lower, transform_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("lower(?)", field(unquote(target_binding_var), ^unquote(key_var))) != ^transform_value
          )

        {:==, {:lower, transform_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("lower(?)", field(unquote(target_binding_var), ^unquote(key_var))) == ^transform_value
          )

        {:not, {:!=, {:lower, transform_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("lower(?)", field(unquote(target_binding_var), ^unquote(key_var))) == ^transform_value
          )

        {:!=, {:lower, transform_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("lower(?)", field(unquote(target_binding_var), ^unquote(key_var))) != ^transform_value
          )

        {:not, {:==, {:upper, transform_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("upper(?)", field(unquote(target_binding_var), ^unquote(key_var))) != ^transform_value
          )

        {:==, {:upper, transform_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("upper(?)", field(unquote(target_binding_var), ^unquote(key_var))) == ^transform_value
          )

        {:not, {:!=, {:upper, transform_value}}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("upper(?)", field(unquote(target_binding_var), ^unquote(key_var))) == ^transform_value
          )

        {:!=, {:upper, transform_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment("upper(?)", field(unquote(target_binding_var), ^unquote(key_var))) != ^transform_value
          )

        _ ->
          nil
      end
    end

    defp string_expr(
           unquote(quoted_binding_head),
           unquote(key_var),
           unquote(negated_var),
           unquote(value_var)
         ) do
      term =
        case unquote(negated_var) do
          :not -> {:not, unquote(value_var)}
          _ -> unquote(value_var)
        end

      case term do
        {:not, {:like, values}} when is_list(values) ->
          patterns = Enum.map(values, &preserve_or_wrap_pattern/1)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment(
              "? LIKE ANY(?)",
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^patterns
            )
          )

        {:like, values} when is_list(values) ->
          patterns = Enum.map(values, &preserve_or_wrap_pattern/1)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment(
              "? LIKE ANY(?)",
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^patterns
            )
          )

        {:not, {:ilike, values}} when is_list(values) ->
          patterns = Enum.map(values, &preserve_or_wrap_pattern/1)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not fragment(
              "? ILIKE ANY(?)",
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^patterns
            )
          )

        {:ilike, values} when is_list(values) ->
          patterns = Enum.map(values, &preserve_or_wrap_pattern/1)

          dynamic(
            [unquote_splicing(quoted_binding_body)],
            fragment(
              "? ILIKE ANY(?)",
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^patterns
            )
          )

        {:not, {:like, string_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not like(
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^preserve_or_wrap_pattern(string_value)
            )
          )

        {:like, string_value} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            like(
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^preserve_or_wrap_pattern(string_value)
            )
          )

        {:not, {:ilike, string_value}} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            not ilike(
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^preserve_or_wrap_pattern(string_value)
            )
          )

        {:ilike, string_value} ->
          dynamic(
            [unquote_splicing(quoted_binding_body)],
            ilike(
              field(unquote(target_binding_var), ^unquote(key_var)),
              ^preserve_or_wrap_pattern(string_value)
            )
          )

        _ ->
          nil
      end
    end
  end

  def dynamic_expr(_selected_binding, _key, _negated, _term, _opts), do: nil

  defp normalize_term({transform, value}) when transform in [:lower, :upper] do
    {:==, {transform, value}}
  end

  defp normalize_term({op, value}) do
    normalized_op = normalize_operator(op)
    {normalized_op, normalize_term_value(normalized_op, value)}
  end

  defp normalize_term(value), do: {:==, value}

  defp normalize_operator(:eq), do: :==
  defp normalize_operator(:ne), do: :!=
  defp normalize_operator(:gt), do: :>
  defp normalize_operator(:gte), do: :>=
  defp normalize_operator(:lt), do: :<
  defp normalize_operator(:lte), do: :<=
  defp normalize_operator(op), do: op

  defp normalize_term_value(op, {nested_op, nested_value}) when op in @aggregate_helpers do
    {normalize_operator(nested_op), nested_value}
  end

  defp normalize_term_value(_op, value), do: value

  defp family_for(:in, _term), do: :membership

  defp family_for(op, value) when op in @equality_operators and is_list(value) do
    :membership
  end

  defp family_for(op, {transform, _term})
       when op in @comparison_operators and transform in [:lower, :upper] do
    :string_transform
  end

  defp family_for(op, term) when op in @string_operators do
    case term do
      {transform, _term} when transform in [:lower, :upper] ->
        :string_transform

      _ ->
        :string
    end
  end

  defp family_for(_op, _term), do: :comparison

  defp preserve_or_wrap_pattern(value) when is_binary(value) do
    if String.contains?(value, ["%", "_"]) do
      value
    else
      "%#{value}%"
    end
  end

  defp preserve_or_wrap_pattern(value) do
    "%#{value}%"
  end
end
