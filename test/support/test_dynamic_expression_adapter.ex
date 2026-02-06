defmodule EctoShorts.TestDynamicExpressionAdapter do
  @moduledoc false

  import Ecto.Query

  def operators, do: []

  def build_dynamic_expression(_source, _binding_selector, key, _expr) do
    dynamic([q], field(q, ^key) == ^:override)
  end
end
