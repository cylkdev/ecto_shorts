defmodule EctoShorts.CommonParams do
  @moduledoc """
  ...
  """
  @moduledoc since: "2.5.0"

  alias EctoShorts.CommonSchemas

  @logger_prefix "EctoShorts.CommonParams"

  # @naive_datetime :naive_datetime
  @utc_datetime :utc_datetime

  @inserted_at_field_name :inserted_at
  @updated_at_field_name :updated_at

  @time_zone_utc "Etc/UTC"

  # @one_hundred 100

  ## Insert API

  @doc """
  ...
  """
  def convert_to_insert_params(query, params, opts) do
    queryable = CommonSchemas.get_schema_queryable(query)

    naive_datetime = maybe_generate_naive_datetime(opts)

    results_errors =
      params
      |> Stream.with_index()
      |> Enum.reduce({[], []}, fn {changes, idx}, {results, errors} ->
        case apply_changes(queryable, changes, naive_datetime, opts) do
          {:ok, res} -> {[res | results], errors}
          {:error, err} -> {results, [{idx, err} | errors]}
        end
      end)

    case results_errors do
      {results, []} -> {:ok, Enum.reverse(results)}
      {_, errors} -> {:error, Enum.reverse(errors)}
    end
  end

  defp prepare_insert_params(schema_data, queryable, datetime, opts) do
    schema_data
    |> schema_to_map()
    |> drop_nil_values()
    |> drop_associations(queryable)
    |> maybe_put_placeholders(opts)
    |> put_timestamp_inserted_at(queryable, datetime, opts)
    |> put_timestamp_updated_at(queryable, datetime, opts)
  end

  defp apply_changes(queryable, {changeset, params}, naive_datetime, opts) when is_struct(changeset, Ecto.Changeset) do
    action = if has_id?(changeset.data), do: :update, else: :insert

    params = drop_associations(params, queryable)

    if Keyword.get(opts, :validate, true) do
      with {:ok, schema_data} <-
        queryable
        |> CommonSchemas.prepare_changeset(changeset, params, opts)
        |> Ecto.Changeset.apply_action(opts[:action] || changeset.action || action) do

        {:ok, prepare_insert_params(schema_data, queryable, naive_datetime, opts)}
      end
    else
      result =
        changeset.data
        |> struct!(Map.merge(changeset.changes, params))
        |> prepare_insert_params(queryable, naive_datetime, opts)

      {:ok, result}
    end
  end

  defp apply_changes(queryable, {%_{} = schema_data, params}, naive_datetime, opts) do
    action = if has_id?(schema_data), do: :update, else: :insert

    params = drop_associations(params, queryable)

    if Keyword.get(opts, :validate, true) do
      with {:ok, schema_data} <-
        queryable
        |> CommonSchemas.prepare_changeset(schema_data, params, opts)
        |> Ecto.Changeset.apply_action(opts[:action] || action) do

        {:ok, prepare_insert_params(schema_data, queryable, naive_datetime, opts)}
      end
    else
      result =
        schema_data
        |> struct!(params)
        |> prepare_insert_params(queryable, naive_datetime, opts)

      {:ok, result}
    end
  end

  defp apply_changes(queryable, params, naive_datetime, opts) do
    action = if has_id?(params), do: :update, else: :insert

    params = drop_associations(params, queryable)

    if Keyword.get(opts, :validate, true) do
      with {:ok, schema_data} <-
        queryable
        |> CommonSchemas.prepare_changeset(struct(queryable), params, opts)
        |> Ecto.Changeset.apply_action(opts[:action] || action) do

        {:ok, prepare_insert_params(schema_data, queryable, naive_datetime, opts)}
      end
    else
      result =
        queryable
        |> struct!(params)
        |> prepare_insert_params(queryable, naive_datetime, opts)

      {:ok, result}
    end
  end

  defp maybe_put_placeholders(params, opts) do
    case opts[:placeholders] do
      nil -> params
      placeholders -> reduce_placeholders(params, placeholders)
    end
  end

  defp reduce_placeholders(params, placeholders) do
    Enum.reduce(placeholders, params, fn {placeholder_key, placeholder_value}, params ->
      case Map.get(params, placeholder_key) do
        nil -> params
        existing_value ->
          if existing_value === placeholder_value do
            put_placeholder(params, placeholder_key)
          else
            params
          end
      end
    end)
  end

  defp put_placeholder(params, key) do
    Map.put(params, key, {:placeholder, key})
  end

  defp put_timestamp_inserted_at(params, queryable, naive_datetime, opts) do
    if naive_datetime && Keyword.get(opts, :timestamp_inserted_at, true) do
      field_name = timestamp_inserted_at_field_name(opts)

      schema_timestamp_type = schema_timestamp_type(queryable, field_name)

      unless schema_timestamp_type do
        raise ArgumentError, "Timestamp field name for inserted_at not found on schema #{inspect(queryable)}, got: #{inspect(field_name)}."
      end

      timestamp_type = opts[:timestamps_type] || schema_timestamp_type

      naive_datetime = maybe_to_utc_datetime(naive_datetime, timestamp_type)

      if has_id?(params) do
        params
      else
        Map.put(params, field_name, naive_datetime)
      end
    else
      params
    end
  end

  defp put_timestamp_updated_at(params, queryable, naive_datetime, opts) do
    if naive_datetime && Keyword.get(opts, :timestamp_updated_at, true) do
      field_name = timestamp_updated_at_field_name(opts)

      schema_timestamp_type = schema_timestamp_type(queryable, field_name)

      unless schema_timestamp_type do
        raise ArgumentError, "Timestamp field name for updated_at not found on schema #{inspect(queryable)}, got: #{inspect(field_name)}."
      end

      timestamp_type = opts[:timestamps_type] || schema_timestamp_type

      naive_datetime = maybe_to_utc_datetime(naive_datetime, timestamp_type)

      Map.put(params, field_name, naive_datetime)
    else
      params
    end
  end

  ## Update API

  @doc """
  ...
  """
  def convert_to_update_params(query, params, opts) when is_map(params) do
    convert_to_update_params(query, Map.to_list(params), opts)
  end

  def convert_to_update_params(query, params, opts) do
    queryable = CommonSchemas.get_schema_queryable(query)

    naive_datetime = maybe_generate_naive_datetime(opts)

    params
    |> Keyword.drop([:id, "id"])
    |> drop_associations(queryable)
    |> normalize_updates()
    |> merge_updates(queryable)
    |> put_set_updated_at(queryable, naive_datetime, opts)
    |> maybe_sort_update_params(opts)
  end

  defp maybe_sort_update_params(params, opts) do
    if Keyword.get(opts, :ordered, true) do
      params
      |> Enum.map(fn {action, params} -> {action, Enum.sort_by(params, fn {key, _} -> key end)} end)
      |> Enum.sort_by(fn {action, _} -> action end)
    else
      params
    end
  end

  defp merge_updates(action_field_values, queryable) do
    action_field_values
    |> Enum.group_by(fn {action, _, _} -> action end)
    |> Enum.reduce([], fn {action, action_field_values}, acc ->
      field_values =
        Enum.map(action_field_values, fn {_action, field, value} ->
          unless queryable.__schema__(:type, field) do
            EctoShorts.Utils.Logger.warning(
              @logger_prefix,
              "The field #{inspect(field)} does not exist on the schema #{inspect(queryable)}"
            )
          end

          {field, value}
        end)

      Keyword.put(acc, action, field_values)
    end)
  end

  defp put_set_updated_at(params, queryable, naive_datetime, opts) do
    if naive_datetime && Keyword.get(opts, :timestamp_updated_at, true) do
      field_name = timestamp_updated_at_field_name(opts)

      schema_timestamp_type = schema_timestamp_type(queryable, field_name)

      unless schema_timestamp_type do
        raise ArgumentError, "Timestamp field name for updated_at not found on schema #{inspect(queryable)}, got: #{inspect(field_name)}."
      end

      timestamp_type = opts[:timestamps_type] || schema_timestamp_type

      datetime = maybe_to_utc_datetime(naive_datetime, timestamp_type)

      Keyword.update(
        params,
        :set,
        [{field_name, datetime}],
        &Keyword.put(&1, field_name, datetime)
      )
    else
      params
    end
  end

  defp normalize_updates(params) do
    normalize_updates(params, [])
  end

  defp normalize_updates([], acc) do
    Enum.reverse(acc)
  end

  defp normalize_updates([head | tail], acc) do
    with acc <- normalize_updates(head, acc) do
      normalize_updates(tail, acc)
    end
  end

  defp normalize_updates({key, params}, acc) when is_map(params) do
    normalize_updates({key, Map.to_list(params)}, acc)
  end

  defp normalize_updates({_key, []}, acc) do
    Enum.reverse(acc)
  end

  defp normalize_updates({key, [head | tail]}, acc) when is_tuple(head) do
    with acc <- normalize_updates({key, head}, acc) do
      normalize_updates({key, tail}, acc)
    end
  end

  defp normalize_updates({key, [head | tail]}, acc) do
    normalize_updates({key, tail}, [query_update(:push, key, head) | acc])
  end

  defp normalize_updates({key, {action, val}}, acc) do
    [query_update(action, key, val) | acc]
  end

  defp normalize_updates({key, val}, acc) do
    [query_update(:set, key, val) | acc]
  end

  defp query_update(:inc, key, val) do
    {:inc, key, val}
  end

  defp query_update(:set, key, val) do
    {:set, key, val}
  end

  defp query_update(:push, key, val) do
    {:push, key, val}
  end

  defp query_update(:pull, key, val) do
    {:pull, key, val}
  end

  ## Helper API

  defp maybe_to_utc_datetime(naive_datetime, type) do
    if type === @utc_datetime do
      DateTime.from_naive!(naive_datetime, @time_zone_utc)
    else
      naive_datetime
    end
  end

  defp maybe_generate_naive_datetime(opts) do
    if Keyword.get(opts, :autogenerate, true) do
      NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second)
    end
  end

  defp timestamp_inserted_at_field_name(opts) do
    opts[:timestamp_inserted_at_field_name] || @inserted_at_field_name
  end

  defp timestamp_updated_at_field_name(opts) do
    opts[:timestamp_updated_at_field_name] || @updated_at_field_name
  end

  defp schema_timestamp_type(queryable, field_name) do
    queryable.__schema__(:type, field_name)
  end

  defp schema_to_map(schema_data) do
    Map.drop(schema_data, [:__meta__, :__struct__])
  end

  defp drop_associations(params, queryable) when is_list(params) do
    Keyword.drop(params, queryable.__schema__(:associations))
  end

  defp drop_associations(params, queryable) do
    Map.drop(params, queryable.__schema__(:associations))
  end

  defp has_id?(params), do: Map.has_key?(params, :id)

  defp drop_nil_values(params) do
    params
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Map.new()
  end
end
