defmodule EctoShorts.CommonFilters.Windows do
  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Utils

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Windows"
  @window_keys [:partition_by, :order_by, :frame]

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:windows, _source, query, selected_binding, params, _opts) do
    normalized_params = Utils.normalize_input(params)
    reduce_params(query, selected_binding, normalized_params)
  end

  defp reduce_params(query, selected_binding, params) when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {window_name, window_definition}, query_acc ->
        apply_window(query_acc, selected_binding, window_name, window_definition)
      end)
    else
      Enum.reduce(params, query, fn
        {window_name, window_definition}, query_acc ->
          apply_window(query_acc, selected_binding, window_name, window_definition)

        other, query_acc ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :windows params to be a map or keyword list, got: #{inspect(other)}"
          )

          query_acc
      end)
    end
  end

  defp reduce_params(query, _selected_binding, value) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :windows params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_window(query, selected_binding, window_name, window_definition) do
    cond do
      not is_atom(window_name) ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected window name to be an atom, got: #{inspect(window_name)}"
        )

        query

      not Keyword.keyword?(window_definition) ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected window definition for #{inspect(window_name)} to be a map or keyword list, got: #{inspect(window_definition)}"
        )

        query

      true ->
        definition = Keyword.take(window_definition, @window_keys)
        partition_by = normalize_partition_by(definition[:partition_by] || [], selected_binding)
        order_by = normalize_order_by(definition[:order_by] || [], selected_binding)
        frame = definition[:frame]

        if is_atom(frame) or is_struct(frame, Ecto.Query.DynamicExpr) do
          apply_window_definition(query, selected_binding, window_name, partition_by, order_by, frame)
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :frame for #{inspect(window_name)} to be an Ecto dynamic expression, got: #{inspect(frame)}"
          )

          query
        end
    end
  end

  defp normalize_partition_by(nil, _selected_binding), do: []

  defp normalize_partition_by(value, selected_binding) when is_atom(value) do
    [dynamic_field_expr(selected_binding, value)]
  end

  defp normalize_partition_by(values, selected_binding) when is_list(values) do
    if Keyword.keyword?(values) do
      values
    else
      Enum.map(values, fn
        value when is_atom(value) ->
          dynamic_field_expr(selected_binding, value)

        other ->
          other
      end)
    end
  end

  defp normalize_partition_by(value, _selected_binding), do: value

  defp normalize_order_by(nil, _selected_binding), do: []

  defp normalize_order_by(value, selected_binding) when is_atom(value) do
    [dynamic_field_expr(selected_binding, value)]
  end

  defp normalize_order_by({direction, field_name}, selected_binding) when is_atom(field_name) do
    [{direction, dynamic_field_expr(selected_binding, field_name)}]
  end

  defp normalize_order_by(values, selected_binding) when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.map(values, fn
        {direction, field_name} when is_atom(field_name) ->
          {direction, dynamic_field_expr(selected_binding, field_name)}

        other ->
          other
      end)
    else
      Enum.map(values, fn
        value when is_atom(value) ->
          dynamic_field_expr(selected_binding, value)

        other ->
          other
      end)
    end
  end

  defp normalize_order_by(value, _selected_binding), do: value

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_window_definition(
           query,
           unquote(quoted_binding_head),
           window_name,
           partition_by,
           order_by,
           nil
         ) do
      Query.windows(
        query,
        [unquote_splicing(quoted_binding_body)],
        [{window_name, [partition_by: ^partition_by, order_by: ^order_by]}]
      )
    end

    defp apply_window_definition(
           query,
           unquote(quoted_binding_head),
           window_name,
           partition_by,
           order_by,
           frame
         ) do
      Query.windows(
        query,
        [unquote_splicing(quoted_binding_body)],
        [{window_name, [partition_by: ^partition_by, order_by: ^order_by, frame: ^frame]}]
      )
    end

    defp dynamic_field_expr(unquote(quoted_binding_head), field_name) do
      Query.dynamic(
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field_name)
      )
    end
  end
end
