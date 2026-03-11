defmodule EctoShorts.CommonFilters.PutQueryPrefix do
  alias Ecto.Query

  def build_query(:put_query_prefix, _source, query, _selected_binding, prefix, _opts) do
    Query.put_query_prefix(query, prefix)
  end
end
