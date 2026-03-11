defmodule EctoShorts.CommonFilters.Last do
  alias EctoShorts.CommonSchema
  alias EctoShorts.Utils

  alias Ecto.Query
  require Ecto.Query

  def build_query(:last, source, query, selected_binding, term, opts)
      when (is_map(term) and not is_struct(term)) or is_list(term) do
    term
    |> Utils.normalize_input()
    |> Enum.reduce(query, fn entry, query_acc ->
      build_query(:last, source, query_acc, selected_binding, entry, opts)
    end)
  end

  def build_query(:last, source, query, _selected_binding, {sort_key, limit}, _opts) do
    sort_keys =
      if is_nil(sort_key) do
        List.wrap(CommonSchema.get_schema_reflection(source, :primary_key) || :id)
      else
        List.wrap(sort_key)
      end

    excluded = Query.exclude(query, :order_by)

    subquery =
      sort_keys
      |> Enum.reduce(excluded, &Query.order_by(&2, desc: ^&1))
      |> Query.limit(^limit)
      |> Query.subquery()

    Enum.reduce(sort_keys, subquery, &Query.order_by(&2, asc: ^&1))
  end

  def build_query(:last, source, query, selected_binding, limit, opts) when is_integer(limit) do
    build_query(:last, source, query, selected_binding, {nil, limit}, opts)
  end
end
