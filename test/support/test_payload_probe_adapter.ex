defmodule EctoShorts.TestPayloadProbeAdapter do
  @moduledoc false

  import Ecto.Query

  def operators, do: [:exists]

  def convert_to_dynamic(source, binding_selector, term) do
    term
    |> normalize_term()
    |> Enum.reduce(nil, fn {k, v}, dyn_left ->
      dyn_right = build_dynamic(source, binding_selector, k, v)

      case {dyn_left, dyn_right} do
        {nil, right} -> right
        {left, right} -> dynamic([], ^left and ^right)
      end
    end)
  end

  defp normalize_term(map) when is_map(map), do: Map.to_list(map)
  defp normalize_term(list) when is_list(list), do: list
  defp normalize_term({k, v}), do: [{k, v}]

  def build_dynamic(_source, _binding_selector, key, expr) do
    send(self(), {:payload_probe_expr, key, expr})
    dynamic([q], not is_nil(field(q, ^:id)))
  end
end
