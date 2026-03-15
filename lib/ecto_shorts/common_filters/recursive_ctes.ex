defmodule EctoShorts.CommonFilters.RecursiveCtes do
  @moduledoc false

  alias Ecto.Query

  def build_query(:recursive_ctes, _source, query, _selected_binding, value, _opts) do
    Query.recursive_ctes(query, value)
  end
end
