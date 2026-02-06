defmodule EctoShorts.Actions.Batch do
  alias EctoShorts.{
    Config,
    CommonFilters,
    CommonSchema
  }

  @cardinalities [:one, :many]

  @doc group: "Batch"
  @doc """
  ...
  """
  def batch(schema, params, batch_keys \\ :id, cardinality \\ :many, opts \\ [])

  def batch(_schema, [], _batch_keys, _cardinality, _opts) do
    %{}
  end

  def batch(schema, params, batch_keys, cardinality, opts)
      when is_list(batch_keys) and cardinality in @cardinalities do
    batch_keys = Enum.uniq(batch_keys)

    case build_batch_params(schema, params, batch_keys, opts) do
      [] ->
        %{}

      batch_params ->
        schema
        |> CommonFilters.convert_params_to_filter(batch_params, opts)
        |> Config.repo!(opts).all(opts)
        |> Enum.group_by(&Map.take(&1, batch_keys))
        |> handle_batch_response(cardinality, batch_keys)
    end
  end

  def batch(schema, params, batch_key, cardinality, opts)
      when cardinality in @cardinalities do
    values =
      params
      |> Enum.map(&normalize_batch_key(&1, batch_key))
      |> Enum.uniq()

    if values === [] do
      %{}
    else
      schema
      |> CommonFilters.convert_params_to_filter(%{batch_key => values}, opts)
      |> Config.repo!(opts).all(opts)
      |> Enum.group_by(&normalize_batch_key(&1, batch_key))
      |> handle_batch_response(cardinality, batch_key)
    end
  end

  defp build_batch_params(_schema, _list_of_params, [], _opts) do
    []
  end

  defp build_batch_params(schema, params_list, batch_keys, opts) do
    query_fields = get_query_fields(opts, schema)

    if Enum.all?(batch_keys, &(&1 in query_fields)) do
      params_list
      |> Enum.map(fn params -> normalize_batch_key(params, batch_keys) end)
      |> Enum.uniq()
      |> Enum.map(&{:or_where, &1})
    else
      raise ArgumentError,
            "Expected batch keys to be a subset of query fields #{inspect(query_fields)}, got: #{inspect(batch_keys)}"
    end
  end

  defp normalize_batch_key(params, keys) when is_list(params) and is_list(keys) do
    params |> Map.new() |> normalize_batch_key(keys)
  end

  defp normalize_batch_key(params, keys) when is_map(params) and is_list(keys) do
    Map.take(params, keys)
  end

  defp normalize_batch_key(params, key) when is_map(params) do
    Map.get(params, key)
  end

  defp normalize_batch_key(value, key) when is_atom(key) do
    %{key => value}
  end

  defp handle_batch_response(records, cardinality, batch_key) do
    records
    |> Enum.map(fn {key, values} ->
      case {cardinality, values} do
        {:one, [value]} ->
          {key, value}

        {:one, _} ->
          raise ArgumentError,
                "Expected at most one value for batch key #{inspect(batch_key)}, got #{length(values)}"

        {_, values} ->
          {key, values}
      end
    end)
    |> Map.new()
  end

  @doc group: "Batch"
  @doc """
  ...
  """
  def batch_preload(schema, entries, keys, opts) do
    {params_list, index_to_key} = extract_lookup_params(entries, keys)

    key_fields = normalize_key_fields(keys)

    fetched_records = batch(schema, params_list, key_fields, :one, opts)

    Enum.reduce(index_to_key, entries, fn {index, batch_key}, acc ->
      case Map.get(fetched_records, batch_key) do
        nil ->
          acc

        record ->
          current_entry = get_in(acc, [Access.at!(index)])
          updated_entry = zip_batch_result(current_entry, record)
          put_in(acc, [Access.at!(index)], updated_entry)
      end
    end)
  end

  defp normalize_key_fields(key) when is_atom(key), do: [key]
  defp normalize_key_fields(keys) when is_list(keys), do: keys
  defp normalize_key_fields(keys), do: keys

  defp zip_batch_result({_find_params, other_params}, record) do
    {record, other_params}
  end

  defp zip_batch_result(original_params, record) do
    {record, original_params}
  end

  defp extract_lookup_params(entries, keys) do
    entries
    |> Stream.with_index()
    |> Enum.reduce({[], %{}}, &reduce_preload_entry(&1, &2, keys))
  end

  defp reduce_preload_entry({entry, index}, {values_acc, index_map}, keys) do
    case normalize_preload_params(entry) do
      nil ->
        {values_acc, index_map}

      params ->
        batch_key = build_batch_key(params, keys)

        if batch_key === %{} do
          {values_acc, index_map}
        else
          {[batch_key | values_acc], Map.put(index_map, index, batch_key)}
        end
    end
  end

  defp normalize_preload_params({params, _other}) do
    normalize_preload_params(params)
  end

  defp normalize_preload_params(params) when is_list(params) do
    Map.new(params)
  end

  defp normalize_preload_params(params) when is_map(params) and not is_struct(params) do
    params
  end

  defp normalize_preload_params(_), do: nil

  defp build_batch_key(params, true) do
    params
  end

  defp build_batch_key(params, keys) when is_list(keys) do
    Map.take(params, keys)
  end

  defp build_batch_key(params, key) when is_atom(key) do
    Map.take(params, [key])
  end

  defp build_batch_key(params, key_fn) when is_function(key_fn) do
    case key_fn.(params) do
      map when is_map(map) -> map
      term -> raise "Expected batch key function to return a map, got: #{inspect(term)}"
    end
  end

  defp get_query_fields(opts, source) do
    Keyword.get(opts, :query_fields, CommonSchema.get_schema_reflection(source, :query_fields))
  end
end
