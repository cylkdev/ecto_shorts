defmodule EctoShorts.Dynamics.Postgres.ScalarExpr do
  import Ecto.Query, only: [dynamic: 2]

  def dynamic_expr({:as, nil}, key, nil, _opts) do
    dynamic([q], is_nil(field(q, ^key)))
  end

  def dynamic_expr({:as, nil}, key, value, _opts) when is_list(value) do
    if Keyword.keyword?(value) do
      nil
    else
      dynamic([q], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({:as, nil}, key, value, _opts) when not is_list(value) do
    dynamic([q], field(q, ^key) == ^value)
  end

  def dynamic_expr({kind, _} = binding_selector, key, nil, _opts) when kind in [:as, :at] do
    dynamic([{^binding_selector, q}], is_nil(field(q, ^key)))
  end

  def dynamic_expr({kind, _} = binding_selector, key, value, _opts)
      when kind in [:as, :at] and is_list(value) do
    if Keyword.keyword?(value) do
      nil
    else
      dynamic([{^binding_selector, q}], field(q, ^key) in ^value)
    end
  end

  def dynamic_expr({kind, _} = binding_selector, key, value, _opts)
      when kind in [:as, :at] and not is_list(value) do
    dynamic([{^binding_selector, q}], field(q, ^key) == ^value)
  end

  def dynamic_expr(_binding_selector, _key, _value, _opts), do: nil
end
