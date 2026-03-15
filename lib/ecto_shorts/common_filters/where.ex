defmodule EctoShorts.CommonFilters.Where do
  alias Ecto.Query
  require Ecto.Query

  def build_query(filter, source, query, selected_binding, term, opts) do
    dyn =
      EctoShorts.Dynamics.Adapters.Postgres.build_dynamic(
        source,
        selected_binding,
        term,
        opts
      )

    case filter do
      :where -> Query.where(query, ^dyn)
      :or_where -> Query.or_where(query, ^dyn)
    end
  end
end
