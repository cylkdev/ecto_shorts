defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:==, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:eq, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:eq, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:!=, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      {:!=, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:ne, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      {:ne, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:not, {:>, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) > ^value))
        end

      {:>, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) > ^value)
        end

      {:not, {:>, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) > ^value))
        end

      {:>, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) > ^value)
        end

      {:not, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) > ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
        end

      {:>, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) > ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
        end

      {:not, {:>=, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        end

      {:>=, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:>=, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        end

      {:>=, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
        end

      {:>=, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
        end

      {:not, {:<, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) < ^value))
        end

      {:<, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) < ^value)
        end

      {:not, {:<, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) < ^value))
        end

      {:<, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) < ^value)
        end

      {:not, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) < ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
        end

      {:<, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) < ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
        end

      {:not, {:<=, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        end

      {:<=, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:<=, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        end

      {:<=, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
        end

      {:<=, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
        end

      {:not, {:gt, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) > ^value))
        end

      {:gt, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) > ^value)
        end

      {:not, {:gt, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) > ^value))
        end

      {:gt, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) > ^value)
        end

      {:not, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) > ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
        end

      {:gt, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) > ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
        end

      {:not, {:gte, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        end

      {:gte, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:gte, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        end

      {:gte, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) >= ^value)
        end

      {:not, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
        end

      {:gte, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) >= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
        end

      {:not, {:lt, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) < ^value))
        end

      {:lt, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) < ^value)
        end

      {:not, {:lt, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) < ^value))
        end

      {:lt, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) < ^value)
        end

      {:not, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) < ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
        end

      {:lt, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) < ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
        end

      {:not, {:lte, {:lower, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        end

      {:lte, {:lower, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:lte, {:upper, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        end

      {:lte, {:upper, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) <= ^value)
        end

      {:not, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (field(q, ^key) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
        end

      {:lte, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) <= ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
        end

      {:not, {:in, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      {:in, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:not, {:like, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        else
          dynamic(
            [{^binding_alias, q}],
            not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        end

      {:like, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        else
          dynamic(
            [{^binding_alias, q}],
            fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        end

      {:not, {:like, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{value}%"))
        end

      {:like, value} ->
        if is_nil(binding_alias) do
          dynamic([q], like(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{value}%"))
        end

      {:not, {:ilike, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        else
          dynamic(
            [{^binding_alias, q}],
            not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        end

      {:ilike, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        else
          dynamic(
            [{^binding_alias, q}],
            fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
          )
        end

      {:not, {:ilike, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{value}%"))
        end

      {:ilike, value} ->
        if is_nil(binding_alias) do
          dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{value}%"))
        end
    end
  end

  def dynamic_expr({:at, 1}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, value) do
    case value do
      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:>, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:>, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:>, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:>, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:>=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:>=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:<, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:<, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:<, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:<=, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:<=, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))

      {:gt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)

      {:not, {:gt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))

      {:gt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))

      {:gte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))

      {:gte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))

      {:lt, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)

      {:not, {:lt, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))

      {:lt, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, {:lower, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))

      {:lte, {:lower, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, {:upper, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))

      {:lte, {:upper, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          not fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          fragment("? LIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          not fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          fragment("? ILIKE ANY(?)", field(q, ^key), ^Enum.map(value, fn value -> "%#{value}%" end))
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end