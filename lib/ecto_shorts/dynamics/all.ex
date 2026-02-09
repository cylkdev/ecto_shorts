defmodule EctoShorts.Dynamics.All do
  @moduledoc false

  alias EctoShorts.CommonFilters
  import Ecto.Query, only: [select: 3]

  @doc false
  def resolve_value(source, key, value, opts) when is_map(value) and not is_struct(value) do
    resolve_value(source, key, Map.to_list(value), opts)
  end

  def resolve_value(source, key, value, opts) when is_list(value) do
    if Keyword.keyword?(value) do
      if Keyword.has_key?(value, :source) or Keyword.has_key?(value, :query) do
        payload_source = Keyword.get(value, :source, source)
        filter_params = Keyword.get(value, :query, [])

        payload_source
        |> CommonFilters.convert_params_to_filter(filter_params, opts)
        |> ensure_scalar_select(key)
      else
        Enum.map(value, fn {inner_op, rhs} ->
          {inner_op, resolve_rhs(source, key, rhs, opts)}
        end)
      end
    else
      value
    end
  end

  def resolve_value(source, key, {inner_op, rhs}, opts) when is_atom(inner_op) do
    {inner_op, resolve_rhs(source, key, rhs, opts)}
  end

  def resolve_value(_source, _key, value, _opts), do: value

  defp resolve_rhs(source, key, rhs, opts) when is_map(rhs) and not is_struct(rhs) do
    if Map.has_key?(rhs, :source) or Map.has_key?(rhs, :query) do
      payload_source = Map.get(rhs, :source, source)
      filter_params = Map.get(rhs, :query, [])

      payload_source
      |> CommonFilters.convert_params_to_filter(filter_params, opts)
      |> ensure_scalar_select(key)
    else
      rhs
    end
  end

  defp resolve_rhs(source, key, rhs, opts) when is_list(rhs) do
    if Keyword.keyword?(rhs) and (Keyword.has_key?(rhs, :source) or Keyword.has_key?(rhs, :query)) do
      payload_source = Keyword.get(rhs, :source, source)
      filter_params = Keyword.get(rhs, :query, [])

      payload_source
      |> CommonFilters.convert_params_to_filter(filter_params, opts)
      |> ensure_scalar_select(key)
    else
      rhs
    end
  end

  defp resolve_rhs(_source, _key, rhs, _opts), do: rhs

  defp ensure_scalar_select(query, field_name)
       when is_struct(query, Ecto.Query) and is_atom(field_name) and not is_nil(field_name) do
    case query.select do
      nil ->
        select(query, [q], field(q, ^field_name))

      _ ->
        query
    end
  end

  defp ensure_scalar_select(query, _field_name), do: query
end
