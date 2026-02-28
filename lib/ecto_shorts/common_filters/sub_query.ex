defmodule EctoShorts.CommonFilters.SubQuery do
  @moduledoc """
  Wraps a filtered query in `Ecto.Query.subquery/1`.
  """

  alias Ecto.Query

  require Ecto.Query

  @doc false
  def build(_schema_source, :subquery, query, _binding_selector, _params, _opts) do
    Query.subquery(query)
  end
end
