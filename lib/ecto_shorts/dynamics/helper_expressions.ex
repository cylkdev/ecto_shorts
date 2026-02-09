defmodule EctoShorts.Dynamics.HelperExpressions do
  @moduledoc false
  alias EctoShorts.CommonFilters

  def build(source, field_name, value, opts) do
    prepare_expression(source, field_name, value, opts)
  end

  defp prepare_expression(source, field_name, expression, opts) do
    case expression do
      map when is_map(map) and not is_struct(map) ->
        prepare_expression(source, field_name, Map.to_list(map), opts)

      list when is_list(list) ->
        if not Keyword.keyword?(list) do
          expression
        else
          if Keyword.has_key?(list, :source) or Keyword.has_key?(list, :query) do
            build_subquery_from_payload(source, field_name, list, opts)
          else
            Enum.map(list, fn {inner_op, rhs} ->
              {inner_op, prepare_expression(source, field_name, rhs, opts)}
            end)
          end
        end

      {inner_op, rhs} when is_atom(inner_op) ->
        {inner_op, prepare_expression(source, field_name, rhs, opts)}

      _ ->
        expression
    end
  end

  defp build_subquery_from_payload(source, field_name, payload, opts) do
    payload_source = Keyword.get(payload, :source, source)
    original_filter_params = Keyword.get(payload, :query, [])

    filter_params =
      case original_filter_params do
        map when is_map(map) and not is_struct(map) ->
          Map.put_new(map, :select, field_name)

        list when is_list(list) ->
          if Keyword.keyword?(list), do: Keyword.put_new(list, :select, field_name), else: list

        _ ->
          original_filter_params
      end

    CommonFilters.convert_params_to_filter(payload_source, filter_params, opts)
  end
end
