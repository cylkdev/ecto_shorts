defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.String do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type negated :: :not | nil
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn =
              if is_nil(binding_alias) do
                dynamic([q], like(field(q, ^key), ^"%#{string_value}%"))
              else
                dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{string_value}%"))
              end

            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        if is_nil(binding_alias) do
          dynamic([q], not (^grouped_dynamic))
        else
          dynamic([{^binding_alias, q}], not (^grouped_dynamic))
        end

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn =
            if is_nil(binding_alias) do
              dynamic([q], like(field(q, ^key), ^"%#{string_value}%"))
            else
              dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{string_value}%"))
            end

          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

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
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn =
              if is_nil(binding_alias) do
                dynamic([q], ilike(field(q, ^key), ^"%#{string_value}%"))
              else
                dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{string_value}%"))
              end

            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        if is_nil(binding_alias) do
          dynamic([q], not (^grouped_dynamic))
        else
          dynamic([{^binding_alias, q}], not (^grouped_dynamic))
        end

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn =
            if is_nil(binding_alias) do
              dynamic([q], ilike(field(q, ^key), ^"%#{string_value}%"))
            else
              dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{string_value}%"))
            end

          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

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

  def dynamic_expr({:at, 1}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:ilike, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))

      {:ilike, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:like, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, _, _, _, q], not (^grouped_dynamic))

      {:like, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

      {:not, {:like, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))

      {:like, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))

      {:not, {:ilike, value}} when is_list(value) ->
        grouped_dynamic =
          Enum.reduce(value, nil, fn string_value, acc ->
            dyn = dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
            EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
          end)

        dynamic([_, _, _, _, _, _, _, _, _, q], not (^grouped_dynamic))

      {:ilike, value} when is_list(value) ->
        Enum.reduce(value, nil, fn string_value, acc ->
          dyn = dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{string_value}%"))
          EctoShorts.CommonFilters.FilterHelpers.merge_dynamic(acc, :or, dyn)
        end)

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