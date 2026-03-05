defmodule EctoShorts.TestDynamicExpressionAdapter do
  @moduledoc false

  import Ecto.Query

  def operators, do: []

  def operator?(key), do: key in operators()

  def build_dynamic(_source, _binding_selector, key, _expr) do
    dynamic([q], field(q, ^key) == ^:override)
  end
end
