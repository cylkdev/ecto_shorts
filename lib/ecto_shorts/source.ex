defmodule EctoShorts.Source do
  @moduledoc since: "3.0.0"
  @moduledoc """
  when this is passed the :from key must point to a name and it returns {:ok, value} or {:error, reason}
  """

  defstruct [:tables]

  @type t :: %__MODULE__{tables: %{optional(String.t()) => term()}}

  @spec new(map() | keyword()) :: t()
  def new(attrs) do
    struct!(__MODULE__, tables: Map.new(attrs[:tables] || []))
  end
end
