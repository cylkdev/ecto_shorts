defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.Membership do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, value) do
    case value do
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

      {:not, {:==, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      {:not, {:eq, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      {:==, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:eq, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:not, {:!=, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:not, {:ne, value}} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      {:!=, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      {:ne, value} when is_list(value) ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end
    end
  end

  def dynamic_expr({:at, 1}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([_, q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([_, q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([_, q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([_, q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([_, q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([_, q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([_, q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([_, q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([_, q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([_, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([_, _, q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([_, _, q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([_, _, q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([_, _, q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([_, _, q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([_, _, q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([_, _, q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([_, _, q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([_, _, q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([_, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([_, _, _, q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, value) do
    case value do
      {:not, {:in, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
      {:in, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:==, value}} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
      {:not, {:eq, value}} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
      {:==, value} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
      {:eq, value} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:!=, value}} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
      {:not, {:ne, value}} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
      {:!=, value} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
      {:ne, value} when is_list(value) -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, value) do
    case value do
      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:==, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:not, {:eq, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:!=, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:ne, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, value) do
    case value do
      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:==, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:not, {:eq, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:!=, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:ne, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, value) do
    case value do
      {:not, {:in, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:in, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:==, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:not, {:eq, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:==, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:eq, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:!=, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:not, {:ne, value}} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)

      {:!=, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)

      {:ne, value} when is_list(value) ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end