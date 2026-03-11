defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.String do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr(
        {:as, binding_alias},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            not fragment(
              "? LIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
          )
        else
          dynamic(
            [{^binding_alias, q}],
            not fragment(
              "? LIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
          )
        end

      {:like, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            fragment(
              "? LIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
          )
        else
          dynamic(
            [{^binding_alias, q}],
            fragment(
              "? LIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
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
            not fragment(
              "? ILIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
          )
        else
          dynamic(
            [{^binding_alias, q}],
            not fragment(
              "? ILIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
          )
        end

      {:ilike, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic(
            [q],
            fragment(
              "? ILIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
          )
        else
          dynamic(
            [{^binding_alias, q}],
            fragment(
              "? ILIKE ANY(?)",
              field(q, ^key),
              ^Enum.map(value, fn value -> "%#{value}%" end)
            )
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

  def dynamic_expr(
        {:at, 1},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 2},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 3},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 4},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 5},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 6},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 7},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 8},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 9},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(
        {:at, 10},
        key,
        negated,
        value
      ) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          not fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:like, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          fragment(
            "? LIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          not fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:ilike, value} when is_list(value) ->
        dynamic(
          [_, _, _, _, _, _, _, _, _, q],
          fragment(
            "? ILIKE ANY(?)",
            field(q, ^key),
            ^Enum.map(value, fn value -> "%#{value}%" end)
          )
        )

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end