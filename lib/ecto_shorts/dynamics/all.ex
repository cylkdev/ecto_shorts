defmodule EctoShorts.Dynamics.All do
  @moduledoc false

  alias EctoShorts.CommonFilters

  require Ecto.Query

  def build_all(source, key, value, opts) when is_map(value) and not is_struct(value) do
    build_all(source, key, Map.to_list(value), opts)
  end

  def build_all(source, key, value, opts) when is_list(value) do
    if Keyword.keyword?(value) do
      if Keyword.has_key?(value, :source) or Keyword.has_key?(value, :query) do
        all_query_from_payload(source, key, value, opts)
      else
        Enum.map(value, fn {inner_op, rhs} ->
          {inner_op, build_all_rhs(source, key, rhs, opts)}
        end)
      end
    else
      value
    end
  end

  def build_all(source, key, {inner_op, rhs}, opts) when is_atom(inner_op) do
    {inner_op, build_all_rhs(source, key, rhs, opts)}
  end

  def build_all(_source, _key, value, _opts), do: value

  defp build_all_rhs(source, key, rhs, opts) when is_map(rhs) and not is_struct(rhs) do
    build_all_rhs(source, key, Map.to_list(rhs), opts)
  end

  defp build_all_rhs(source, key, rhs, opts) when is_list(rhs) do
    if Keyword.keyword?(rhs) and (Keyword.has_key?(rhs, :source) or Keyword.has_key?(rhs, :query)) do
      all_query_from_payload(source, key, rhs, opts)
    else
      rhs
    end
  end

  defp build_all_rhs(_source, _key, rhs, _opts), do: rhs

  defp all_query_from_payload(source, key, payload, opts) do
    payload_source = Keyword.get(payload, :source, source)
    filter_params = Keyword.get(payload, :query, [])

    payload_source
    |> CommonFilters.convert_params_to_filter(filter_params, opts)
    |> ensure_all_scalar_select(key)
  end

  defp ensure_all_scalar_select(query, field_name)
       when is_struct(query, Ecto.Query) and is_atom(field_name) and not is_nil(field_name) do
    case query.select do
      nil ->
        Ecto.Query.select(query, [q], field(q, ^field_name))

      _ ->
        query
    end
  end

  defp ensure_all_scalar_select(query, _field_name), do: query
end
