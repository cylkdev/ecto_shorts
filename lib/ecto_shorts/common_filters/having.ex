defmodule EctoShorts.CommonFilters.Having do
  alias Ecto.Query
  require Ecto.Query

  def build_query(_filter, _source, query, _selected_binding, nil, _opts), do: query

  def build_query(filter, _source, query, _selected_binding, dyn, _opts) do
    case filter do
      :having ->
        Query.having(query, ^dyn)

      :or_having ->
        Query.or_having(query, ^dyn)
    end
  end
end
