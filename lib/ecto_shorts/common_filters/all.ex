defmodule EctoShorts.CommonFilters.All do
  @moduledoc false

  alias EctoShorts.CommonFilters

  @doc false
  def resolve_value(source, value, opts) when is_map(value) and not is_struct(value) do
    resolve_value(source, Map.to_list(value), opts)
  end

  def resolve_value(source, value, opts) when is_list(value) do
    if Keyword.keyword?(value) do
      if Keyword.has_key?(value, :source) or Keyword.has_key?(value, :query) do
        payload_source = Keyword.get(value, :source, source)
        filter_params = Keyword.get(value, :query, [])
        CommonFilters.convert_params_to_filter(payload_source, filter_params, opts)
      else
        Enum.map(value, fn {inner_op, rhs} ->
          {inner_op, resolve_rhs(source, rhs, opts)}
        end)
      end
    else
      value
    end
  end

  def resolve_value(_source, value, _opts), do: value

  defp resolve_rhs(source, rhs, opts) when is_map(rhs) and not is_struct(rhs) do
    if Map.has_key?(rhs, :source) or Map.has_key?(rhs, :query) do
      payload_source = Map.get(rhs, :source, source)
      filter_params = Map.get(rhs, :query, [])
      CommonFilters.convert_params_to_filter(payload_source, filter_params, opts)
    else
      rhs
    end
  end

  defp resolve_rhs(source, rhs, opts) when is_list(rhs) do
    if Keyword.keyword?(rhs) and (Keyword.has_key?(rhs, :source) or Keyword.has_key?(rhs, :query)) do
      payload_source = Keyword.get(rhs, :source, source)
      filter_params = Keyword.get(rhs, :query, [])
      CommonFilters.convert_params_to_filter(payload_source, filter_params, opts)
    else
      rhs
    end
  end

  defp resolve_rhs(_source, rhs, _opts), do: rhs
end
