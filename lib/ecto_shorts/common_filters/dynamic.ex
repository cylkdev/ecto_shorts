defmodule EctoShorts.CommonFilters.Dynamic do
  import Ecto.Query, only: [dynamic: 1]

  def merge_dynamic(nil, _, b), do: b
  def merge_dynamic(a, _, nil), do: a
  def merge_dynamic(a, :and, b), do: dynamic(^a and ^b)
  def merge_dynamic(a, :or, b), do: dynamic(^a or ^b)
end
