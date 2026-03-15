defmodule EctoShorts.CommonFilters.SubQuery do
  alias EctoShorts.CommonFilters
  alias EctoShorts.Utils

  alias Ecto.Query
  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.SubQuery"

  def build_query(:subquery, _source, query, _selected_binding, term, opts) do
    params = Utils.map_to_keyword(term)

    if Keyword.keyword?(params) do
      inner_query =
        CommonFilters.convert_params_to_filter(
          query,
          params,
          opts
        )

      Query.subquery(inner_query)
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :subquery value to be a keyword list or map of filter params, got: #{inspect(term)}"
      )

      query
    end
  end
end
