defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.Comparison do
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

      {:not, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:==, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:not, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:==, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:not, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:eq, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:not, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:eq, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:not, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:!=, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:not, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:!=, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:not, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], is_nil(field(q, ^key)))
        end

      {:ne, nil} ->
        if is_nil(binding_alias) do
          dynamic([q], not is_nil(field(q, ^key)))
        else
          dynamic([{^binding_alias, q}], not is_nil(field(q, ^key)))
        end

      {:not, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) == ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
        end

      {:ne, value} ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) != ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
        end

      {:not, {:avg, {:>, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) > ^value))
        end

      {:avg, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) > ^value)
        end

      {:not, {:avg, {:>=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) >= ^value))
        end

      {:avg, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) >= ^value)
        end

      {:not, {:avg, {:<, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) < ^value))
        end

      {:avg, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) < ^value)
        end

      {:not, {:avg, {:<=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) <= ^value))
        end

      {:avg, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) <= ^value)
        end

      {:not, {:avg, {:gt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) > ^value))
        end

      {:avg, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) > ^value)
        end

      {:not, {:avg, {:gte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) >= ^value))
        end

      {:avg, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) >= ^value)
        end

      {:not, {:avg, {:lt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) < ^value))
        end

      {:avg, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) < ^value)
        end

      {:not, {:avg, {:lte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (avg(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (avg(field(q, ^key)) <= ^value))
        end

      {:avg, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) <= ^value)
        end

      {:not, {:avg, {:==, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^nil)
        end

      {:avg, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^nil)
        end

      {:not, {:avg, {:==, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^value)
        end

      {:avg, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^value)
        end

      {:not, {:avg, {:eq, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^nil)
        end

      {:avg, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^nil)
        end

      {:not, {:avg, {:eq, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^value)
        end

      {:avg, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^value)
        end

      {:not, {:avg, {:!=, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^nil)
        end

      {:avg, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^nil)
        end

      {:not, {:avg, {:!=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^value)
        end

      {:avg, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^value)
        end

      {:not, {:avg, {:ne, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^nil)
        end

      {:avg, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^nil)
        end

      {:not, {:avg, {:ne, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) == ^value)
        end

      {:avg, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], avg(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], avg(field(q, ^key)) != ^value)
        end

      {:not, {:count, {:>, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) > ^value))
        end

      {:count, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) > ^value)
        end

      {:not, {:count, {:>=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) >= ^value))
        end

      {:count, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) >= ^value)
        end

      {:not, {:count, {:<, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) < ^value))
        end

      {:count, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) < ^value)
        end

      {:not, {:count, {:<=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) <= ^value))
        end

      {:count, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) <= ^value)
        end

      {:not, {:count, {:gt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) > ^value))
        end

      {:count, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) > ^value)
        end

      {:not, {:count, {:gte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) >= ^value))
        end

      {:count, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) >= ^value)
        end

      {:not, {:count, {:lt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) < ^value))
        end

      {:count, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) < ^value)
        end

      {:not, {:count, {:lte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (count(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (count(field(q, ^key)) <= ^value))
        end

      {:count, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) <= ^value)
        end

      {:not, {:count, {:==, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^nil)
        end

      {:count, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^nil)
        end

      {:not, {:count, {:==, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^value)
        end

      {:count, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^value)
        end

      {:not, {:count, {:eq, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^nil)
        end

      {:count, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^nil)
        end

      {:not, {:count, {:eq, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^value)
        end

      {:count, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^value)
        end

      {:not, {:count, {:!=, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^nil)
        end

      {:count, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^nil)
        end

      {:not, {:count, {:!=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^value)
        end

      {:count, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^value)
        end

      {:not, {:count, {:ne, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^nil)
        end

      {:count, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^nil)
        end

      {:not, {:count, {:ne, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) == ^value)
        end

      {:count, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], count(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], count(field(q, ^key)) != ^value)
        end

      {:not, {:max, {:>, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) > ^value))
        end

      {:max, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) > ^value)
        end

      {:not, {:max, {:>=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) >= ^value))
        end

      {:max, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) >= ^value)
        end

      {:not, {:max, {:<, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) < ^value))
        end

      {:max, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) < ^value)
        end

      {:not, {:max, {:<=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) <= ^value))
        end

      {:max, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) <= ^value)
        end

      {:not, {:max, {:gt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) > ^value))
        end

      {:max, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) > ^value)
        end

      {:not, {:max, {:gte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) >= ^value))
        end

      {:max, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) >= ^value)
        end

      {:not, {:max, {:lt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) < ^value))
        end

      {:max, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) < ^value)
        end

      {:not, {:max, {:lte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (max(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (max(field(q, ^key)) <= ^value))
        end

      {:max, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) <= ^value)
        end

      {:not, {:max, {:==, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^nil)
        end

      {:max, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^nil)
        end

      {:not, {:max, {:==, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^value)
        end

      {:max, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^value)
        end

      {:not, {:max, {:eq, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^nil)
        end

      {:max, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^nil)
        end

      {:not, {:max, {:eq, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^value)
        end

      {:max, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^value)
        end

      {:not, {:max, {:!=, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^nil)
        end

      {:max, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^nil)
        end

      {:not, {:max, {:!=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^value)
        end

      {:max, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^value)
        end

      {:not, {:max, {:ne, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^nil)
        end

      {:max, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^nil)
        end

      {:not, {:max, {:ne, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) == ^value)
        end

      {:max, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], max(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], max(field(q, ^key)) != ^value)
        end

      {:not, {:min, {:>, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) > ^value))
        end

      {:min, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) > ^value)
        end

      {:not, {:min, {:>=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) >= ^value))
        end

      {:min, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) >= ^value)
        end

      {:not, {:min, {:<, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) < ^value))
        end

      {:min, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) < ^value)
        end

      {:not, {:min, {:<=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) <= ^value))
        end

      {:min, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) <= ^value)
        end

      {:not, {:min, {:gt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) > ^value))
        end

      {:min, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) > ^value)
        end

      {:not, {:min, {:gte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) >= ^value))
        end

      {:min, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) >= ^value)
        end

      {:not, {:min, {:lt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) < ^value))
        end

      {:min, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) < ^value)
        end

      {:not, {:min, {:lte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (min(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (min(field(q, ^key)) <= ^value))
        end

      {:min, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) <= ^value)
        end

      {:not, {:min, {:==, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^nil)
        end

      {:min, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^nil)
        end

      {:not, {:min, {:==, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^value)
        end

      {:min, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^value)
        end

      {:not, {:min, {:eq, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^nil)
        end

      {:min, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^nil)
        end

      {:not, {:min, {:eq, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^value)
        end

      {:min, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^value)
        end

      {:not, {:min, {:!=, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^nil)
        end

      {:min, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^nil)
        end

      {:not, {:min, {:!=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^value)
        end

      {:min, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^value)
        end

      {:not, {:min, {:ne, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^nil)
        end

      {:min, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^nil)
        end

      {:not, {:min, {:ne, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) == ^value)
        end

      {:min, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], min(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], min(field(q, ^key)) != ^value)
        end

      {:not, {:sum, {:>, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) > ^value))
        end

      {:sum, {:>, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) > ^value)
        end

      {:not, {:sum, {:>=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) >= ^value))
        end

      {:sum, {:>=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) >= ^value)
        end

      {:not, {:sum, {:<, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) < ^value))
        end

      {:sum, {:<, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) < ^value)
        end

      {:not, {:sum, {:<=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) <= ^value))
        end

      {:sum, {:<=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) <= ^value)
        end

      {:not, {:sum, {:gt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) > ^value))
        end

      {:sum, {:gt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) > ^value)
        end

      {:not, {:sum, {:gte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) >= ^value))
        end

      {:sum, {:gte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) >= ^value)
        end

      {:not, {:sum, {:lt, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) < ^value))
        end

      {:sum, {:lt, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) < ^value)
        end

      {:not, {:sum, {:lte, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], not (sum(field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (sum(field(q, ^key)) <= ^value))
        end

      {:sum, {:lte, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) <= ^value)
        end

      {:not, {:sum, {:==, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^nil)
        end

      {:sum, {:==, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^nil)
        end

      {:not, {:sum, {:==, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^value)
        end

      {:sum, {:==, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^value)
        end

      {:not, {:sum, {:eq, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^nil)
        end

      {:sum, {:eq, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^nil)
        end

      {:not, {:sum, {:eq, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^value)
        end

      {:sum, {:eq, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^value)
        end

      {:not, {:sum, {:!=, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^nil)
        end

      {:sum, {:!=, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^nil)
        end

      {:not, {:sum, {:!=, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^value)
        end

      {:sum, {:!=, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^value)
        end

      {:not, {:sum, {:ne, nil}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^nil)
        end

      {:sum, {:ne, nil}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^nil)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^nil)
        end

      {:not, {:sum, {:ne, value}}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) == ^value)
        end

      {:sum, {:ne, value}} ->
        if is_nil(binding_alias) do
          dynamic([q], sum(field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], sum(field(q, ^key)) != ^value)
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
      {:not, {:>, value}} -> dynamic([q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} -> dynamic([_, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([_, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([_, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([_, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([_, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([_, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([_, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([_, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([_, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([_, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([_, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([_, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([_, q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([_, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([_, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([_, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([_, q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([_, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([_, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([_, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([_, q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([_, q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([_, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([_, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([_, q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([_, q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([_, q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([_, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([_, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([_, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([_, q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([_, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([_, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([_, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([_, q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([_, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([_, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([_, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([_, q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([_, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([_, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([_, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([_, q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([_, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([_, q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([_, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([_, q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([_, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([_, q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([_, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([_, q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([_, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([_, q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([_, q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([_, q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([_, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([_, q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([_, q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([_, q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([_, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([_, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([_, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([_, q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([_, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([_, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([_, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([_, q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([_, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([_, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([_, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([_, q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([_, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([_, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([_, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([_, q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([_, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([_, q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([_, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([_, q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([_, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([_, q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([_, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([_, q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([_, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([_, q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([_, q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([_, q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([_, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([_, q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([_, q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([_, q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([_, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([_, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([_, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([_, q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([_, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([_, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([_, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([_, q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([_, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([_, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([_, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([_, q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([_, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([_, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([_, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([_, q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([_, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([_, q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([_, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([_, q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([_, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([_, q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([_, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([_, q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([_, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([_, q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([_, q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([_, q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([_, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([_, q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([_, q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([_, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([_, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([_, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([_, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([_, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([_, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([_, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([_, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([_, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([_, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([_, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([_, q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([_, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([_, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([_, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([_, q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([_, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([_, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([_, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([_, q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([_, q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([_, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([_, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([_, q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([_, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} -> dynamic([_, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([_, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([_, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([_, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([_, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([_, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([_, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([_, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([_, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([_, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([_, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([_, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([_, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([_, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([_, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([_, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([_, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([_, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([_, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([_, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([_, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([_, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([_, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([_, _, q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([_, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([_, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([_, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([_, _, q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([_, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([_, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([_, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([_, _, q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([_, _, q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([_, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([_, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([_, _, q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([_, _, q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([_, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([_, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([_, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([_, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([_, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([_, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([_, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([_, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([_, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([_, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([_, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([_, _, q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([_, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([_, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([_, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([_, _, q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([_, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([_, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([_, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([_, _, q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([_, _, q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([_, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([_, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([_, _, q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([_, _, q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([_, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([_, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([_, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([_, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([_, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([_, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([_, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([_, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([_, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([_, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([_, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([_, _, q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([_, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([_, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([_, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([_, _, q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([_, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([_, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([_, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([_, _, q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([_, _, q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([_, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([_, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([_, _, q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([_, _, q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([_, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([_, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([_, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([_, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([_, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([_, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([_, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([_, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([_, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([_, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([_, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} -> dynamic([_, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([_, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([_, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([_, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([_, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([_, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([_, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([_, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([_, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([_, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([_, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([_, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([_, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([_, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([_, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([_, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([_, _, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([_, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([_, _, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([_, _, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([_, _, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([_, _, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([_, _, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([_, _, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([_, _, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([_, _, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([_, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([_, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([_, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([_, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([_, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([_, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([_, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([_, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([_, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([_, _, _, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:>, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:>=, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:<, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:<, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:<=, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
      {:gt, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
      {:not, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
      {:gte, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
      {:not, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
      {:lt, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
      {:not, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
      {:lte, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
      {:not, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:==, nil} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:==, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:==, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:eq, nil} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:not, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:eq, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:not, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:!=, nil} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:!=, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], is_nil(field(q, ^key)))
      {:ne, nil} -> dynamic([_, _, _, _, _, _, q], not is_nil(field(q, ^key)))
      {:not, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
      {:ne, value} -> dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
      {:not, {:avg, {:>, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:>, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:>=, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:<, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:<, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:<=, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:gt, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))
      {:avg, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)
      {:not, {:avg, {:gte, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))
      {:avg, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)
      {:not, {:avg, {:lt, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))
      {:avg, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)
      {:not, {:avg, {:lte, value}}} -> dynamic([_, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))
      {:avg, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)
      {:not, {:avg, {:==, nil}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:==, value}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:==, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:eq, nil}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:avg, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:not, {:avg, {:eq, value}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:avg, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:not, {:avg, {:!=, nil}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:!=, value}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:avg, {:ne, nil}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)
      {:avg, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)
      {:not, {:avg, {:ne, value}}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)
      {:avg, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)
      {:not, {:count, {:>, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:>, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:>=, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:<, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:<, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:<=, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:gt, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))
      {:count, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) > ^value)
      {:not, {:count, {:gte, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))
      {:count, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)
      {:not, {:count, {:lt, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))
      {:count, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) < ^value)
      {:not, {:count, {:lte, value}}} -> dynamic([_, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))
      {:count, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)
      {:not, {:count, {:==, nil}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:==, value}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:==, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:eq, nil}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:count, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:not, {:count, {:eq, value}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:count, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:not, {:count, {:!=, nil}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:!=, value}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:count, {:ne, nil}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)
      {:count, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)
      {:not, {:count, {:ne, value}}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) == ^value)
      {:count, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], count(field(q, ^key)) != ^value)
      {:not, {:max, {:>, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:>, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:>=, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:<, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:<, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:<=, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:gt, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))
      {:max, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) > ^value)
      {:not, {:max, {:gte, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))
      {:max, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)
      {:not, {:max, {:lt, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))
      {:max, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) < ^value)
      {:not, {:max, {:lte, value}}} -> dynamic([_, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))
      {:max, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)
      {:not, {:max, {:==, nil}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:==, value}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:==, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:eq, nil}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:max, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:not, {:max, {:eq, value}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:max, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:not, {:max, {:!=, nil}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:!=, value}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:max, {:ne, nil}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)
      {:max, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)
      {:not, {:max, {:ne, value}}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) == ^value)
      {:max, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], max(field(q, ^key)) != ^value)
      {:not, {:min, {:>, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:>, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:>=, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:<, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:<, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:<=, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:gt, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))
      {:min, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) > ^value)
      {:not, {:min, {:gte, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))
      {:min, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)
      {:not, {:min, {:lt, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))
      {:min, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) < ^value)
      {:not, {:min, {:lte, value}}} -> dynamic([_, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))
      {:min, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)
      {:not, {:min, {:==, nil}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:==, value}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:==, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:eq, nil}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:min, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:not, {:min, {:eq, value}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:min, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:not, {:min, {:!=, nil}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:!=, value}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:min, {:ne, nil}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)
      {:min, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)
      {:not, {:min, {:ne, value}}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) == ^value)
      {:min, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], min(field(q, ^key)) != ^value)
      {:not, {:sum, {:>, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:>, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:>=, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:>=, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:<, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:<, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:<=, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:<=, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:gt, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))
      {:sum, {:gt, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)
      {:not, {:sum, {:gte, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))
      {:sum, {:gte, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)
      {:not, {:sum, {:lt, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))
      {:sum, {:lt, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)
      {:not, {:sum, {:lte, value}}} -> dynamic([_, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))
      {:sum, {:lte, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)
      {:not, {:sum, {:==, nil}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:==, nil}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:==, value}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:==, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:eq, nil}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:sum, {:eq, nil}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:not, {:sum, {:eq, value}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:sum, {:eq, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:not, {:sum, {:!=, nil}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:!=, nil}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:!=, value}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:!=, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
      {:not, {:sum, {:ne, nil}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)
      {:sum, {:ne, nil}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)
      {:not, {:sum, {:ne, value}}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)
      {:sum, {:ne, value}} -> dynamic([_, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:avg, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))

      {:avg, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)

      {:not, {:avg, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))

      {:avg, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)

      {:not, {:avg, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))

      {:avg, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)

      {:not, {:avg, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))

      {:avg, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)

      {:not, {:avg, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))

      {:avg, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)

      {:not, {:avg, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))

      {:avg, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)

      {:not, {:avg, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))

      {:avg, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)

      {:not, {:avg, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))

      {:avg, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)

      {:not, {:avg, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:avg, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:not, {:avg, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:avg, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:not, {:avg, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:avg, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:not, {:avg, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:avg, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:not, {:avg, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:avg, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:not, {:avg, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:avg, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:not, {:avg, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:avg, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:not, {:avg, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:avg, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:not, {:count, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))

      {:count, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) > ^value)

      {:not, {:count, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))

      {:count, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)

      {:not, {:count, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))

      {:count, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) < ^value)

      {:not, {:count, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))

      {:count, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)

      {:not, {:count, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))

      {:count, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) > ^value)

      {:not, {:count, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))

      {:count, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)

      {:not, {:count, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))

      {:count, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) < ^value)

      {:not, {:count, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))

      {:count, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)

      {:not, {:count, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:count, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:not, {:count, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:count, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:not, {:count, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:count, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:not, {:count, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:count, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:not, {:count, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:count, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:not, {:count, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:count, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:not, {:count, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:count, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:not, {:count, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:count, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:not, {:max, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))

      {:max, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) > ^value)

      {:not, {:max, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))

      {:max, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)

      {:not, {:max, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))

      {:max, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) < ^value)

      {:not, {:max, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))

      {:max, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)

      {:not, {:max, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))

      {:max, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) > ^value)

      {:not, {:max, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))

      {:max, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)

      {:not, {:max, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))

      {:max, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) < ^value)

      {:not, {:max, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))

      {:max, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)

      {:not, {:max, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:max, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:not, {:max, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:max, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:not, {:max, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:max, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:not, {:max, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:max, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:not, {:max, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:max, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:not, {:max, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:max, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:not, {:max, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:max, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:not, {:max, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:max, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:not, {:min, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))

      {:min, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) > ^value)

      {:not, {:min, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))

      {:min, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)

      {:not, {:min, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))

      {:min, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) < ^value)

      {:not, {:min, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))

      {:min, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)

      {:not, {:min, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))

      {:min, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) > ^value)

      {:not, {:min, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))

      {:min, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)

      {:not, {:min, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))

      {:min, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) < ^value)

      {:not, {:min, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))

      {:min, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)

      {:not, {:min, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:min, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:not, {:min, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:min, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:not, {:min, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:min, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:not, {:min, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:min, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:not, {:min, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:min, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:not, {:min, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:min, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:not, {:min, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:min, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:not, {:min, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:min, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:not, {:sum, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))

      {:sum, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)

      {:not, {:sum, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))

      {:sum, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)

      {:not, {:sum, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))

      {:sum, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)

      {:not, {:sum, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))

      {:sum, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)

      {:not, {:sum, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))

      {:sum, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)

      {:not, {:sum, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))

      {:sum, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)

      {:not, {:sum, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))

      {:sum, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)

      {:not, {:sum, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))

      {:sum, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)

      {:not, {:sum, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:sum, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:not, {:sum, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:sum, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:not, {:sum, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:sum, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:not, {:sum, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:sum, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:not, {:sum, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:sum, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:not, {:sum, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:sum, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:not, {:sum, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:sum, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:not, {:sum, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:sum, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:avg, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))

      {:avg, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)

      {:not, {:avg, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))

      {:avg, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)

      {:not, {:avg, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))

      {:avg, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)

      {:not, {:avg, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))

      {:avg, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)

      {:not, {:avg, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))

      {:avg, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)

      {:not, {:avg, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))

      {:avg, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)

      {:not, {:avg, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))

      {:avg, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)

      {:not, {:avg, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))

      {:avg, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)

      {:not, {:avg, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:avg, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:not, {:avg, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:avg, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:not, {:avg, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:avg, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:not, {:avg, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:avg, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:not, {:avg, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:avg, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:not, {:avg, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:avg, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:not, {:avg, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:avg, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:not, {:avg, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:avg, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:not, {:count, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))

      {:count, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) > ^value)

      {:not, {:count, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))

      {:count, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)

      {:not, {:count, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))

      {:count, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) < ^value)

      {:not, {:count, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))

      {:count, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)

      {:not, {:count, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))

      {:count, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) > ^value)

      {:not, {:count, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))

      {:count, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)

      {:not, {:count, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))

      {:count, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) < ^value)

      {:not, {:count, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))

      {:count, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)

      {:not, {:count, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:count, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:not, {:count, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:count, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:not, {:count, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:count, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:not, {:count, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:count, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:not, {:count, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:count, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:not, {:count, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:count, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:not, {:count, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:count, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:not, {:count, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:count, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:not, {:max, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))

      {:max, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) > ^value)

      {:not, {:max, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))

      {:max, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)

      {:not, {:max, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))

      {:max, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) < ^value)

      {:not, {:max, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))

      {:max, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)

      {:not, {:max, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))

      {:max, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) > ^value)

      {:not, {:max, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))

      {:max, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)

      {:not, {:max, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))

      {:max, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) < ^value)

      {:not, {:max, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))

      {:max, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)

      {:not, {:max, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:max, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:not, {:max, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:max, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:not, {:max, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:max, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:not, {:max, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:max, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:not, {:max, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:max, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:not, {:max, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:max, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:not, {:max, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:max, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:not, {:max, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:max, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:not, {:min, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))

      {:min, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) > ^value)

      {:not, {:min, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))

      {:min, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)

      {:not, {:min, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))

      {:min, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) < ^value)

      {:not, {:min, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))

      {:min, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)

      {:not, {:min, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))

      {:min, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) > ^value)

      {:not, {:min, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))

      {:min, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)

      {:not, {:min, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))

      {:min, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) < ^value)

      {:not, {:min, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))

      {:min, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)

      {:not, {:min, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:min, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:not, {:min, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:min, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:not, {:min, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:min, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:not, {:min, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:min, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:not, {:min, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:min, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:not, {:min, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:min, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:not, {:min, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:min, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:not, {:min, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:min, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:not, {:sum, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))

      {:sum, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)

      {:not, {:sum, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))

      {:sum, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)

      {:not, {:sum, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))

      {:sum, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)

      {:not, {:sum, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))

      {:sum, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)

      {:not, {:sum, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))

      {:sum, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)

      {:not, {:sum, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))

      {:sum, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)

      {:not, {:sum, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))

      {:sum, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)

      {:not, {:sum, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))

      {:sum, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)

      {:not, {:sum, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:sum, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:not, {:sum, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:sum, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:not, {:sum, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:sum, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:not, {:sum, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:sum, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:not, {:sum, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:sum, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:not, {:sum, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:sum, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:not, {:sum, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:sum, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:not, {:sum, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:sum, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, negated, value) do
    term =
      case negated do
        :not -> {:not, value}
        _ -> value
      end

    case term do
      {:not, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:>, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:>=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:<, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:<=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))

      {:gt, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)

      {:not, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))

      {:gte, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)

      {:not, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))

      {:lt, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)

      {:not, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))

      {:lte, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)

      {:not, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:==, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:==, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:eq, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:not, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:eq, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:not, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:!=, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:!=, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], is_nil(field(q, ^key)))

      {:ne, nil} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not is_nil(field(q, ^key)))

      {:not, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)

      {:ne, value} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)

      {:not, {:avg, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))

      {:avg, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)

      {:not, {:avg, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))

      {:avg, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)

      {:not, {:avg, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))

      {:avg, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)

      {:not, {:avg, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))

      {:avg, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)

      {:not, {:avg, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) > ^value))

      {:avg, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) > ^value)

      {:not, {:avg, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) >= ^value))

      {:avg, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) >= ^value)

      {:not, {:avg, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) < ^value))

      {:avg, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) < ^value)

      {:not, {:avg, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (avg(field(q, ^key)) <= ^value))

      {:avg, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) <= ^value)

      {:not, {:avg, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:avg, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:not, {:avg, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:avg, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:not, {:avg, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:avg, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:not, {:avg, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:avg, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:not, {:avg, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:avg, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:not, {:avg, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:avg, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:not, {:avg, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^nil)

      {:avg, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^nil)

      {:not, {:avg, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) == ^value)

      {:avg, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], avg(field(q, ^key)) != ^value)

      {:not, {:count, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))

      {:count, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) > ^value)

      {:not, {:count, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))

      {:count, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)

      {:not, {:count, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))

      {:count, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) < ^value)

      {:not, {:count, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))

      {:count, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)

      {:not, {:count, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) > ^value))

      {:count, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) > ^value)

      {:not, {:count, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) >= ^value))

      {:count, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) >= ^value)

      {:not, {:count, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) < ^value))

      {:count, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) < ^value)

      {:not, {:count, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (count(field(q, ^key)) <= ^value))

      {:count, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) <= ^value)

      {:not, {:count, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:count, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:not, {:count, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:count, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:not, {:count, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:count, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:not, {:count, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:count, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:not, {:count, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:count, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:not, {:count, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:count, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:not, {:count, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^nil)

      {:count, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^nil)

      {:not, {:count, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) == ^value)

      {:count, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], count(field(q, ^key)) != ^value)

      {:not, {:max, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))

      {:max, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) > ^value)

      {:not, {:max, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))

      {:max, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)

      {:not, {:max, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))

      {:max, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) < ^value)

      {:not, {:max, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))

      {:max, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)

      {:not, {:max, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) > ^value))

      {:max, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) > ^value)

      {:not, {:max, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) >= ^value))

      {:max, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) >= ^value)

      {:not, {:max, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) < ^value))

      {:max, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) < ^value)

      {:not, {:max, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (max(field(q, ^key)) <= ^value))

      {:max, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) <= ^value)

      {:not, {:max, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:max, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:not, {:max, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:max, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:not, {:max, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:max, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:not, {:max, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:max, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:not, {:max, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:max, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:not, {:max, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:max, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:not, {:max, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^nil)

      {:max, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^nil)

      {:not, {:max, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) == ^value)

      {:max, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], max(field(q, ^key)) != ^value)

      {:not, {:min, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))

      {:min, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) > ^value)

      {:not, {:min, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))

      {:min, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)

      {:not, {:min, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))

      {:min, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) < ^value)

      {:not, {:min, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))

      {:min, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)

      {:not, {:min, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) > ^value))

      {:min, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) > ^value)

      {:not, {:min, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) >= ^value))

      {:min, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) >= ^value)

      {:not, {:min, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) < ^value))

      {:min, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) < ^value)

      {:not, {:min, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (min(field(q, ^key)) <= ^value))

      {:min, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) <= ^value)

      {:not, {:min, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:min, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:not, {:min, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:min, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:not, {:min, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:min, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:not, {:min, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:min, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:not, {:min, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:min, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:not, {:min, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:min, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:not, {:min, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^nil)

      {:min, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^nil)

      {:not, {:min, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) == ^value)

      {:min, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], min(field(q, ^key)) != ^value)

      {:not, {:sum, {:>, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))

      {:sum, {:>, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)

      {:not, {:sum, {:>=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))

      {:sum, {:>=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)

      {:not, {:sum, {:<, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))

      {:sum, {:<, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)

      {:not, {:sum, {:<=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))

      {:sum, {:<=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)

      {:not, {:sum, {:gt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) > ^value))

      {:sum, {:gt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) > ^value)

      {:not, {:sum, {:gte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) >= ^value))

      {:sum, {:gte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) >= ^value)

      {:not, {:sum, {:lt, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) < ^value))

      {:sum, {:lt, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) < ^value)

      {:not, {:sum, {:lte, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], not (sum(field(q, ^key)) <= ^value))

      {:sum, {:lte, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) <= ^value)

      {:not, {:sum, {:==, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:sum, {:==, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:not, {:sum, {:==, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:sum, {:==, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:not, {:sum, {:eq, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:sum, {:eq, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:not, {:sum, {:eq, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:sum, {:eq, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:not, {:sum, {:!=, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:sum, {:!=, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:not, {:sum, {:!=, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:sum, {:!=, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)

      {:not, {:sum, {:ne, nil}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^nil)

      {:sum, {:ne, nil}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^nil)

      {:not, {:sum, {:ne, value}}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) == ^value)

      {:sum, {:ne, value}} ->
        dynamic([_, _, _, _, _, _, _, _, _, q], sum(field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr(_, _, _, _) do
    nil
  end
end