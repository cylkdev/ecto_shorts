defmodule EctoShorts.CommonFilters.Windows do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindParams

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.Windows"
  @binding_selector_key :bind
  @window_keys [:partition_by, :order_by, :frame]

  @doc false
  def build(_schema_source, :windows, query, binding_selector, params, _opts) do
    reduce_windows(query, binding_selector, params)
  end

  defp reduce_windows(query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_windows(query, binding_selector, Map.to_list(params))
  end

  defp reduce_windows(query, binding_selector, {@binding_selector_key, bind_params}) do
    reduce_windows_bind(query, binding_selector, bind_params)
  end

  defp reduce_windows(query, binding_selector, params) when is_list(params) do
    cond do
      Keyword.keyword?(params) ->
        case Enum.split_with(params, fn {k, _} -> k === @binding_selector_key end) do
          {[], window_entries} ->
            reduce_window_entries(query, binding_selector, window_entries)

          {binding_entries, []} ->
            Enum.reduce(binding_entries, query, fn entry, query_acc ->
              reduce_windows(query_acc, binding_selector, entry)
            end)

          {binding_entries, window_entries} ->
            query_with_windows = reduce_window_entries(query, binding_selector, window_entries)

            Enum.reduce(binding_entries, query_with_windows, fn entry, query_acc ->
              reduce_windows(query_acc, binding_selector, entry)
            end)
        end

      not Keyword.keyword?(params) ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected :windows params to be a keyword list of window definitions, got: #{inspect(params)}"
        )

        query
    end
  end

  defp reduce_windows(query, binding_selector, {window_name, window_definition}) do
    apply_windows_expr(query, binding_selector, window_name, window_definition)
  end

  defp reduce_windows(query, _binding_selector, value) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :windows params to be a keyword list/map of window definitions, got: #{inspect(value)}"
    )

    query
  end

  defp reduce_windows_bind(query, _binding_selector, bind_params) do
    BindParams.reduce_submodule_bind_params(query, bind_params, fn q, {mode, target}, value ->
      reduce_windows(q, {mode, target}, value)
    end)
  end

  defp reduce_window_entries(query, binding_selector, entries) do
    Enum.reduce(entries, query, fn entry, query_acc ->
      reduce_windows(query_acc, binding_selector, entry)
    end)
  end

  defp apply_windows_expr(query, _binding_selector, window_name, _window_definition)
       when not is_atom(window_name) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected window name to be an atom, got: #{inspect(window_name)}"
    )

    query
  end

  defp apply_windows_expr(query, binding_selector, window_name, window_definition) do
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

        partition_by =
          definition
          |> Keyword.get(:partition_by, [])
          |> normalize_partition_by(binding_selector)

        order_by =
          definition
          |> Keyword.get(:order_by, [])
          |> normalize_order_by(binding_selector)

        frame = Keyword.get(definition, :frame)

        if is_nil(frame) do
          compose_window(query, binding_selector, window_name, partition_by, order_by)
        else
          compose_window(query, binding_selector, window_name, partition_by, order_by, frame)
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

  defp normalize_partition_by(nil, _binding_selector), do: []

  defp normalize_partition_by(value, binding_selector) when is_atom(value) do
    [apply_dynamic_expr(binding_selector, value)]
  end

  defp normalize_partition_by(value, binding_selector)
       when is_map(value) and not is_struct(value) do
    normalize_partition_by(Map.to_list(value), binding_selector)
  end

  defp normalize_partition_by(values, binding_selector) when is_list(values) do
    if Keyword.keyword?(values) do
      values
    else
      Enum.map(values, fn
        value when is_atom(value) ->
          apply_dynamic_expr(binding_selector, value)

        other ->
          other
      end)
    end
  end

  defp normalize_partition_by(value, _binding_selector), do: value

  defp normalize_order_by(nil, _binding_selector), do: []

  defp normalize_order_by(value, binding_selector) when is_atom(value) do
    [apply_dynamic_expr(binding_selector, value)]
  end

  defp normalize_order_by({direction, field_name}, binding_selector) when is_atom(field_name) do
    [{direction, apply_dynamic_expr(binding_selector, field_name)}]
  end

  defp normalize_order_by(value, binding_selector)
       when is_map(value) and not is_struct(value) do
    normalize_order_by(Map.to_list(value), binding_selector)
  end

  defp normalize_order_by(values, binding_selector) when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.map(values, fn
        {direction, field_name} when is_atom(field_name) ->
          {direction, apply_dynamic_expr(binding_selector, field_name)}

        other ->
          other
      end)
    else
      Enum.map(values, fn
        value when is_atom(value) ->
          apply_dynamic_expr(binding_selector, value)

        other ->
          other
      end)
    end
  end

  defp normalize_order_by(value, _binding_selector), do: value

  EctoShorts.Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp compose_window(
             query,
             unquote(quoted_binding_head),
             window_name,
             partition_by,
             order_by
           ) do
        Query.windows(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{window_name, [partition_by: ^partition_by, order_by: ^order_by]}]
        )
      end

      defp compose_window(
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

      defp apply_dynamic_expr(unquote(quoted_binding_head), field_name) do
        Query.dynamic(
          [unquote_splicing(quoted_binding_body)],
          field(unquote(target_binding_var), ^field_name)
        )
      end
  end
end
