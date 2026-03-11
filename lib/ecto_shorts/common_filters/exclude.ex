defmodule EctoShorts.CommonFilters.Exclude do
  alias Ecto.Query

  def build_query(:exclude, _source, query, _selected_binding, entries, _opts) do
    entries
    |> List.wrap()
    |> Enum.reduce(query, fn field, query_acc ->
      Query.exclude(query_acc, field)
    end)
  end
end
