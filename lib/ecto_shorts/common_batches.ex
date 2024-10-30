defmodule EctoShorts.CommonBatches do
  @moduledoc """
  ...
  """
  @moduledoc since: "2.5.0"

  alias EctoShorts.{
    CommonFilters,
    Config
  }

  @bag :bag
  @set :set

  @doc """
  ...
  """
  @doc since: "2.5.0"
  def to_collection(enum, field, @bag), do: Enum.group_by(enum, &Map.fetch!(&1, field))
  def to_collection(enum, field, @set), do: Map.new(enum, &{Map.fetch!(&1, field), &1})

  @doc """
  ...
  """
  @doc since: "2.5.0"
  def batch_all(query, field, values, params \\ %{}, type \\ @set, opts \\ []) do
    query
    |> CommonFilters.convert_params_to_filter(Map.merge(params, %{field => values}))
    |> Config.repo!(opts).all(opts)
    |> to_collection(field, type)
  end
end
