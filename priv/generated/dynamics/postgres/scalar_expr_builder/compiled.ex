defmodule EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled do
  @moduledoc false
  import Ecto.Query, only: [dynamic: 2]

  @type selected_binding :: {:as | :at, term()}
  @type key :: atom()
  @type value :: term()
  @type compose_res :: Ecto.Query.t() | nil

  @doc false
  def dynamic_expr({:as, binding_alias}, key, {:not, {:==, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) == ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) == ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:==, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) == ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:==, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:eq, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) == ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) == ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) == ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) == ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:eq, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) == ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) == ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:eq, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) == ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) == ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:!=, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) != ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) != ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:!=, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) != ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:!=, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) != ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ne, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) != ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) != ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) != ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) != ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ne, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) != ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) != ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ne, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) != ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) != ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) > ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) > ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) > ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) > ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>=, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) >= ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) >= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:>=, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:>=, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) < ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) < ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) < ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) < ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<=, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) <= ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) <= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:<=, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:<=, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gt, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) > ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) > ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) > ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) > ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gt, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) > ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) > ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gt, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) > ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) > ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gte, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) >= ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) >= ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, {transform, value}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) >= ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) >= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:gte, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) >= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) >= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:gte, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) >= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) >= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lt, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) < ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) < ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) < ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) < ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lt, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) < ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) < ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lt, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) < ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) < ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lte, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("lower(?)", field(q, ^key)) <= ^value))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        else
          dynamic([{^binding_alias, q}], not (fragment("upper(?)", field(q, ^key)) <= ^value))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, {transform, value}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("lower(?)", field(q, ^key)) <= ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
        else
          dynamic([{^binding_alias, q}], fragment("upper(?)", field(q, ^key)) <= ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:lte, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not (field(q, ^key) <= ^value))
    else
      dynamic([{^binding_alias, q}], not (field(q, ^key) <= ^value))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:lte, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) <= ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) <= ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:in, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) not in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], field(q, ^key) in ^value)
        else
          dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:in, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) not in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:in, value}) do
    if is_nil(binding_alias) do
      dynamic([q], field(q, ^key) in ^value)
    else
      dynamic([{^binding_alias, q}], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:like, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{value}%"))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{value}%"))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, {transform, value}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], like(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{value}%"))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], like(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{value}%"))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:like, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:like, value}) do
    if is_nil(binding_alias) do
      dynamic([q], like(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ilike, {transform, value}}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{value}%"))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{value}%"))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, {transform, value}})
      when transform in [:lower, :upper] do
    case transform do
      :lower ->
        if is_nil(binding_alias) do
          dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{value}%"))
        end

      :upper ->
        if is_nil(binding_alias) do
          dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
        else
          dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{value}%"))
        end
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:not, {:ilike, value}}) do
    if is_nil(binding_alias) do
      dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:as, binding_alias}, key, {:ilike, value}) do
    if is_nil(binding_alias) do
      dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    else
      dynamic([{^binding_alias, q}], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:==, value}}) do
    dynamic([q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:==, value}) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:eq, value}}) do
    dynamic([q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 1}, key, {:eq, value}) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:!=, value}}) do
    dynamic([q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:!=, value}) do
    dynamic([q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ne, value}}) do
    dynamic([q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 1}, key, {:ne, value}) do
    dynamic([q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>, value}}) do
    dynamic([q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 1}, key, {:>, value}) do
    dynamic([q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:>=, value}}) do
    dynamic([q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:>=, value}) do
    dynamic([q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<, value}}) do
    dynamic([q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 1}, key, {:<, value}) do
    dynamic([q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:<=, value}}) do
    dynamic([q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:<=, value}) do
    dynamic([q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gt, value}}) do
    dynamic([q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 1}, key, {:gt, value}) do
    dynamic([q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:gte, value}}) do
    dynamic([q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:gte, value}) do
    dynamic([q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lt, value}}) do
    dynamic([q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 1}, key, {:lt, value}) do
    dynamic([q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 1}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:lte, value}}) do
    dynamic([q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 1}, key, {:lte, value}) do
    dynamic([q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], field(q, ^key) not in ^value)
      :upper -> dynamic([q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], field(q, ^key) in ^value)
      :upper -> dynamic([q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:in, value}}) do
    dynamic([q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 1}, key, {:in, value}) do
    dynamic([q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 1}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:like, value}}) do
    dynamic([q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 1}, key, {:like, value}) do
    dynamic([q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 1}, key, {:not, {:ilike, value}}) do
    dynamic([q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 1}, key, {:ilike, value}) do
    dynamic([q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:==, value}}) do
    dynamic([_, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:==, value}) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:eq, value}}) do
    dynamic([_, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 2}, key, {:eq, value}) do
    dynamic([_, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:!=, value}}) do
    dynamic([_, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:!=, value}) do
    dynamic([_, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ne, value}}) do
    dynamic([_, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 2}, key, {:ne, value}) do
    dynamic([_, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>, value}}) do
    dynamic([_, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 2}, key, {:>, value}) do
    dynamic([_, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:>=, value}}) do
    dynamic([_, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:>=, value}) do
    dynamic([_, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<, value}}) do
    dynamic([_, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 2}, key, {:<, value}) do
    dynamic([_, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:<=, value}}) do
    dynamic([_, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:<=, value}) do
    dynamic([_, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gt, value}}) do
    dynamic([_, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 2}, key, {:gt, value}) do
    dynamic([_, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:gte, value}}) do
    dynamic([_, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:gte, value}) do
    dynamic([_, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lt, value}}) do
    dynamic([_, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 2}, key, {:lt, value}) do
    dynamic([_, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 2}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:lte, value}}) do
    dynamic([_, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 2}, key, {:lte, value}) do
    dynamic([_, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:in, value}}) do
    dynamic([_, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 2}, key, {:in, value}) do
    dynamic([_, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 2}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:like, value}}) do
    dynamic([_, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:like, value}) do
    dynamic([_, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 2}, key, {:not, {:ilike, value}}) do
    dynamic([_, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 2}, key, {:ilike, value}) do
    dynamic([_, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:==, value}}) do
    dynamic([_, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:==, value}) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:eq, value}}) do
    dynamic([_, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 3}, key, {:eq, value}) do
    dynamic([_, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:!=, value}}) do
    dynamic([_, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:!=, value}) do
    dynamic([_, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ne, value}}) do
    dynamic([_, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 3}, key, {:ne, value}) do
    dynamic([_, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>, value}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 3}, key, {:>, value}) do
    dynamic([_, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:>=, value}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:>=, value}) do
    dynamic([_, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<, value}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 3}, key, {:<, value}) do
    dynamic([_, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:<=, value}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:<=, value}) do
    dynamic([_, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gt, value}}) do
    dynamic([_, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 3}, key, {:gt, value}) do
    dynamic([_, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:gte, value}}) do
    dynamic([_, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:gte, value}) do
    dynamic([_, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lt, value}}) do
    dynamic([_, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 3}, key, {:lt, value}) do
    dynamic([_, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 3}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:lte, value}}) do
    dynamic([_, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 3}, key, {:lte, value}) do
    dynamic([_, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:in, value}}) do
    dynamic([_, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 3}, key, {:in, value}) do
    dynamic([_, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 3}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:like, value}}) do
    dynamic([_, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:like, value}) do
    dynamic([_, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 3}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 3}, key, {:ilike, value}) do
    dynamic([_, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:==, value}) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 4}, key, {:eq, value}) do
    dynamic([_, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:!=, value}) do
    dynamic([_, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 4}, key, {:ne, value}) do
    dynamic([_, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 4}, key, {:>, value}) do
    dynamic([_, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:>=, value}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 4}, key, {:<, value}) do
    dynamic([_, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:<=, value}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 4}, key, {:gt, value}) do
    dynamic([_, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:gte, value}) do
    dynamic([_, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 4}, key, {:lt, value}) do
    dynamic([_, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 4}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 4}, key, {:lte, value}) do
    dynamic([_, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 4}, key, {:in, value}) do
    dynamic([_, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 4}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:like, value}) do
    dynamic([_, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 4}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 4}, key, {:ilike, value}) do
    dynamic([_, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:==, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 5}, key, {:eq, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:!=, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 5}, key, {:ne, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 5}, key, {:>, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:>=, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 5}, key, {:<, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:<=, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 5}, key, {:gt, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:gte, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 5}, key, {:lt, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 5}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 5}, key, {:lte, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 5}, key, {:in, value}) do
    dynamic([_, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 5}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:like, value}) do
    dynamic([_, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 5}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 5}, key, {:ilike, value}) do
    dynamic([_, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:==, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 6}, key, {:eq, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:!=, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 6}, key, {:ne, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 6}, key, {:>, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:>=, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 6}, key, {:<, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:<=, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 6}, key, {:gt, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:gte, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 6}, key, {:lt, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 6}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 6}, key, {:lte, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 6}, key, {:in, value}) do
    dynamic([_, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 6}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:like, value}) do
    dynamic([_, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 6}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 6}, key, {:ilike, value}) do
    dynamic([_, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:==, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 7}, key, {:eq, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:!=, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 7}, key, {:ne, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 7}, key, {:>, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:>=, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 7}, key, {:<, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:<=, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 7}, key, {:gt, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:gte, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 7}, key, {:lt, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 7}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 7}, key, {:lte, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 7}, key, {:in, value}) do
    dynamic([_, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 7}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:like, value}) do
    dynamic([_, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 7}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 7}, key, {:ilike, value}) do
    dynamic([_, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:==, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 8}, key, {:eq, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:!=, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 8}, key, {:ne, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 8}, key, {:>, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:>=, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 8}, key, {:<, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:<=, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 8}, key, {:gt, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:gte, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 8}, key, {:lt, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 8}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 8}, key, {:lte, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 8}, key, {:in, value}) do
    dynamic([_, _, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 8}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:like, value}) do
    dynamic([_, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 8}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 8}, key, {:ilike, value}) do
    dynamic([_, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:==, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 9}, key, {:eq, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:!=, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 9}, key, {:ne, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 9}, key, {:>, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:>=, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 9}, key, {:<, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:<=, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 9}, key, {:gt, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:gte, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 9}, key, {:lt, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 9}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 9}, key, {:lte, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 9}, key, {:in, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 9}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:like, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 9}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 9}, key, {:ilike, value}) do
    dynamic([_, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:==, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:==, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:==, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:==, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:eq, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) == ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) == ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:eq, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) == ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:eq, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) == ^value))
  end

  def dynamic_expr({:at, 10}, key, {:eq, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) == ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:!=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:!=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:!=, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:!=, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ne, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) != ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) != ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:ne, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) != ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ne, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) != ^value))
  end

  def dynamic_expr({:at, 10}, key, {:ne, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) != ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:>, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 10}, key, {:>, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:>=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:>=, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:>=, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:<, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 10}, key, {:<, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<=, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:<=, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:<=, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:<=, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) > ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) > ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:gt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) > ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) > ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gt, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) > ^value))
  end

  def dynamic_expr({:at, 10}, key, {:gt, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) > ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) >= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) >= ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:gte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) >= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) >= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:gte, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) >= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:gte, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) >= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lt, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) < ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) < ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:lt, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) < ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) < ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lt, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) < ^value))
  end

  def dynamic_expr({:at, 10}, key, {:lt, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) < ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lte, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("lower(?)", field(q, ^key)) <= ^value))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not (fragment("upper(?)", field(q, ^key)) <= ^value))
    end
  end

  def dynamic_expr({:at, 10}, key, {:lte, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("lower(?)", field(q, ^key)) <= ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], fragment("upper(?)", field(q, ^key)) <= ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:lte, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not (field(q, ^key) <= ^value))
  end

  def dynamic_expr({:at, 10}, key, {:lte, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) <= ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:in, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:in, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:in, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) not in ^value)
  end

  def dynamic_expr({:at, 10}, key, {:in, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], field(q, ^key) in ^value)
  end

  def dynamic_expr({:at, 10}, key, {:not, {:like, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, {:like, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:like, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:like, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], like(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ilike, {transform, value}}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, {:ilike, {transform, value}}) when transform in [:lower, :upper] do
    case transform do
      :lower -> dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
      :upper -> dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
    end
  end

  def dynamic_expr({:at, 10}, key, {:not, {:ilike, value}}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], not ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr({:at, 10}, key, {:ilike, value}) do
    dynamic([_, _, _, _, _, _, _, _, _, q], ilike(field(q, ^key), ^"%#{value}%"))
  end

  def dynamic_expr(_, _, _) do
    nil
  end
end