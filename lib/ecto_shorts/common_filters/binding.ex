defmodule EctoShorts.CommonFilters.FilterHelpers do
  import Ecto.Query, only: [dynamic: 1]

  def merge_dynamic(nil, _, b), do: b
  def merge_dynamic(a, _, nil), do: a
  def merge_dynamic(a, :and, b), do: dynamic(^a and ^b)
  def merge_dynamic(a, :or, b), do: dynamic(^a or ^b)

  def binding_selector?({:as, nil}), do: true
  def binding_selector?({:as, name}) when is_atom(name), do: true
  def binding_selector?({:at, position}) when is_integer(position) and position >= 1, do: true
  def binding_selector?(_), do: false
end
