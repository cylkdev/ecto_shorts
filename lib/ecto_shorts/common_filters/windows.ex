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
    normalized_params = Utils.map_to_list(params)
    reduce_entries(query, selected_binding, normalized_params)
  end

  defp reduce_entries(query, selected_binding, params) when is_list(params) do
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

  defp reduce_entries(query, _selected_binding, value) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :windows params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_window(query, _selected_binding, window_name, _window_definition)
       when not is_atom(window_name) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected window name to be an atom, got: #{inspect(window_name)}"
    )

    query
  end

  defp apply_window(query, selected_binding, window_name, window_definition) do
    case normalize_window_definition(window_definition) do
      {:ok, normalized_definition} ->
        unknown_keys =
          normalized_definition
          |> Keyword.keys()
          |> Enum.reject(&(&1 in @window_keys))

        if unknown_keys !== [] do
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Ignoring unsupported window keys #{inspect(unknown_keys)} for #{inspect(window_name)}"
          )
        end

        definition = Keyword.take(normalized_definition, @window_keys)
        partition_by = normalize_partition_by(Keyword.get(definition, :partition_by, []), selected_binding)
        order_by = normalize_order_by(Keyword.get(definition, :order_by, []), selected_binding)

        with {:ok, frame} <- normalize_frame(window_name, Keyword.get(definition, :frame)) do
          apply_window_definition(query, selected_binding, window_name, partition_by, order_by, frame)
        else
          :error -> query
        end

      :error ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected window definition for #{inspect(window_name)} to be a map or keyword list, got: #{inspect(window_definition)}"
        )

        query
    end
  end

  defp normalize_window_definition(value) when is_map(value) and not is_struct(value),
    do: {:ok, Map.to_list(value)}

  defp normalize_window_definition(value) when is_list(value) do
    if Keyword.keyword?(value), do: {:ok, value}, else: :error
  end

  defp normalize_window_definition(_value), do: :error

  defp normalize_partition_by(nil, _selected_binding), do: []

  defp normalize_partition_by(value, selected_binding) when is_atom(value) do
    [compose(selected_binding, value)]
  end

  defp normalize_partition_by(value, selected_binding)
       when is_map(value) and not is_struct(value) do
    normalize_partition_by(Map.to_list(value), selected_binding)
  end

  defp normalize_partition_by(values, selected_binding) when is_list(values) do
    if Keyword.keyword?(values) do
      values
    else
      Enum.map(values, fn
        value when is_atom(value) ->
          compose(selected_binding, value)

        other ->
          other
      end)
    end
  end

  defp normalize_partition_by(value, _selected_binding), do: value

  defp normalize_order_by(nil, _selected_binding), do: []

  defp normalize_order_by(value, selected_binding) when is_atom(value) do
    [compose(selected_binding, value)]
  end

  defp normalize_order_by({direction, field_name}, selected_binding) when is_atom(field_name) do
    [{direction, compose(selected_binding, field_name)}]
  end

  defp normalize_order_by(value, selected_binding)
       when is_map(value) and not is_struct(value) do
    normalize_order_by(Map.to_list(value), selected_binding)
  end

  defp normalize_order_by(values, selected_binding) when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.map(values, fn
        {direction, field_name} when is_atom(field_name) ->
          {direction, compose(selected_binding, field_name)}

        other ->
          other
      end)
    else
      Enum.map(values, fn
        value when is_atom(value) ->
          compose(selected_binding, value)

        other ->
          other
      end)
    end
  end

  defp normalize_order_by(value, _selected_binding), do: value

  defp normalize_frame(_window_name, nil), do: {:ok, nil}
  defp normalize_frame(_window_name, %Ecto.Query.DynamicExpr{} = frame), do: {:ok, frame}

  defp normalize_frame(window_name, frame) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :frame for #{inspect(window_name)} to be an Ecto dynamic expression, got: #{inspect(frame)}"
    )

    :error
  end

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

    defp compose(unquote(quoted_binding_head), field_name) do
      Query.dynamic(
        [unquote_splicing(quoted_binding_body)],
        field(unquote(target_binding_var), ^field_name)
      )
    end
  end
end
