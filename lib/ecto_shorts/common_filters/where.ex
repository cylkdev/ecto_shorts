defmodule EctoShorts.CommonFilters.Where do
  alias Ecto.Query
  require Ecto.Query

  def build_query(filter, _source, query, _selected_binding, dyn, _opts) do
    case filter do
      :where ->
        Query.where(query, ^dyn)

      :or_where ->
        Query.or_where(query, ^dyn)
    end
  end
end
