defmodule EctoShorts.TestPayloadProbeAdapter do
  @moduledoc false

  import Ecto.Query

  def operators, do: [:exists]

  def build_dynamic(_source, _binding_selector, key, expr) do
    send(self(), {:payload_probe_expr, key, expr})
    dynamic([q], not is_nil(field(q, ^:id)))
  end
end
