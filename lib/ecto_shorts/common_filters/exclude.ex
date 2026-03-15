defmodule EctoShorts.CommonFilters.Exclude do
  @moduledoc false

  alias Ecto.Query

  def build_query(:exclude, _source, query, _selected_binding, term, _opts) do
    term
    |> List.wrap()
    |> Enum.reduce(query, fn field, query_acc ->
      Query.exclude(query_acc, field)
    end)
  end
end
