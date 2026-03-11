defmodule EctoShorts.CommonFilters.SubQuery do
  alias Ecto.Query
  alias EctoShorts.CommonFilters

  require Ecto.Query

  def build_query(:subquery, _source, query, _selected_binding, params, opts)
      when is_map(params) and not is_struct(params) do
    build_query(:subquery, nil, query, nil, Map.to_list(params), opts)
  end

  def build_query(:subquery, _source, query, _selected_binding, params, opts) when is_list(params) do
    inner_query =
      CommonFilters.convert_params_to_filter(
        query,
        params,
        opts
      )

    Query.subquery(inner_query)
  end

  def build_query(:subquery, _source, query, _selected_binding, _params, _opts) do
    query
  end
end
