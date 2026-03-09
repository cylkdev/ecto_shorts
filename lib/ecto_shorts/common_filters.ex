defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"

  alias EctoShorts.CommonSchema
  alias EctoShorts.Adapters.Postgres

  alias Ecto.Query
  require Ecto.Query

  @default_selected_binding {:as, nil}

  def convert_params_to_filter(source, params, opts) do
    query = CommonSchema.to_query(source)
    Enum.reduce(params, query, &apply_filters(source, &2, @default_selected_binding, &1, opts))
  end

  defp apply_filters(source, query, _selected_binding, {bind_op, params}, opts) when bind_op in [:as, :at] do
    Enum.reduce(params, query, fn {key, value}, query_acc ->
      apply_filters(source, query_acc, {bind_op, key}, value, opts)
    end)
  end

  defp apply_filters(source, query, selected_binding, {key, value}, opts) do
    build_query(source, query, selected_binding, {key, value}, opts)
  end

  defp apply_filters(source, query, selected_binding, term, opts) do
    cond do
      is_map(term) and not is_struct(term) ->
        apply_filters(source, query, selected_binding, Map.to_list(term), opts)

      true ->
        Enum.reduce(term, query, &apply_filters(source, &2, selected_binding, &1, opts))
    end
  end

  defp build_query(source, query, selected_binding, term, opts) do
    dyn =
      Postgres.build_dynamic(
        source,
        selected_binding,
        term,
        opts
      )

    Query.where(query, ^dyn)
  end
end
