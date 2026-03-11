defmodule EctoShorts.CommonFilters.Last do
  alias Ecto.Query
  alias EctoShorts.CommonSchema

  require Ecto.Query

  def build_query(:last, source, query, _selected_binding, entries, opts)
      when is_map(entries) and not is_struct(entries) do
    build_query(:last, source, query, nil, Map.to_list(entries), opts)
  end

  def build_query(:last, source, query, _selected_binding, entries, opts) when is_list(entries) do
    Enum.reduce(entries, query, fn entry, query_acc ->
      build_query(:last, source, query_acc, nil, entry, opts)
    end)
  end

  def build_query(:last, source, query, _selected_binding, {sort_key, limit}, _opts) do
    sort_keys =
      if is_nil(sort_key) do
        List.wrap(CommonSchema.get_schema_reflection(source, :primary_key) || :id)
      else
        List.wrap(sort_key)
      end

    subquery =
      sort_keys
      |> Enum.reduce(Query.exclude(query, :order_by), &Query.order_by(&2, desc: ^&1))
      |> Query.limit(^limit)
      |> Query.subquery()

    Enum.reduce(sort_keys, subquery, &Query.order_by(&2, asc: ^&1))
  end

  def build_query(:last, source, query, selected_binding, limit, opts) do
    build_query(:last, source, query, selected_binding, {nil, limit}, opts)
  end
end
