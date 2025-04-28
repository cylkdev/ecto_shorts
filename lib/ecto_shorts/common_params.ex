defmodule EctoShorts.CommonParams do
  @moduledoc false

  alias Ecto.Changeset
  alias EctoShorts.CommonSchemas

  @utc_datetime :utc_datetime
  @naive_datetime :naive_datetime

  @inserted_at :inserted_at
  @updated_at :updated_at

  @doc """
  ...
  """
  def convert_to_update_all_params(query, params, opts \\ []) do
    utc_now = datetime_utc_now()

    params
    |> normalize_updates(query)
    |> group_updates_by_action()
    |> flatten_updates()
    |> maybe_set_updated_at(utc_now, query, opts)
  end

  defp maybe_set_updated_at(keyword, datetime, query, opts) do
    updated_at_source = opts[:updated_at] || opts[:updated_at_source] || @updated_at

    datetime = dump_timestamp_updated_at(datetime, updated_at_source, query, opts)

    Keyword.update(
      keyword,
      :set,
      [{updated_at_source, datetime}],
      &Keyword.put(&1, updated_at_source, datetime)
    )
  end

  defp flatten_updates(grouped_tuples) do
    Enum.map(grouped_tuples, fn {action, tuples} ->
      {action, Keyword.new(tuples, fn {_action, field, value} -> {field, value} end)}
    end)
  end

  defp group_updates_by_action(tuples) do
    Enum.group_by(tuples, fn {action, _field, _value} -> action end)
  end

  defp normalize_updates(params, query) do
    Enum.reduce(params, [], &normalize_update(&1, query, &2))
  end

  defp normalize_update({field, params}, query, acc) when is_list(params) or is_map(params) do
    if field in CommonSchemas.get_schema_reflection(query, :query_fields) do
      params
      |> Enum.reduce(acc, fn {action, value}, acc ->
        [update_invoc(action, field, value) | acc]
      end)
      |> Enum.reverse()
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{CommonSchemas.get_schema_queryable(query)}'."
      )

      acc
    end
  end

  defp normalize_update({field, {action, value}}, query, acc) do
    if field in CommonSchemas.get_schema_reflection(query, :query_fields) do
      [update_invoc(action, field, value) | acc]
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{CommonSchemas.get_schema_queryable(query)}'."
      )

      acc
    end
  end

  defp normalize_update({field, value}, query, acc) do
    if field in CommonSchemas.get_schema_reflection(query, :query_fields) do
      [update_invoc(:set, field, value) | acc]
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{CommonSchemas.get_schema_queryable(query)}'."
      )

      acc
    end
  end

  defp update_invoc(:inc, field, value) when is_integer(value),
    do: {:inc, field, value}

  defp update_invoc(:push, field, value) when is_list(value),
    do: {:push, field, value}

  defp update_invoc(:pull, field, value) when is_list(value),
    do: {:pull, field, value}

  defp update_invoc(:set, field, value), do: {:set, field, value}

  @doc """

  ## Placeholders

  If an existing value matches the placeholder value the existing value
  will be replaced with the place holder tuple.

  ## Options

    * `:placeholders` - A map where each key is the if the field on the
      schema and the value is the placeholder value.

    * `:on_placeholder_conflict` - Sets the behaviour of what should happen if a
      placeholder was given and a value already exists. Can be one of:

        * `:nothing` - Does nothing and leaves the existing value as is.

        * `{:replace, keys}` - Replaces the existing value with the placeholder
          if the key is in one of the keys specified.

        * `:replace_all` - Replaces all existing values with the placeholder value.

    * `:inserted_at` - ...

    * `:inserted_at_source` - ...

    * `:inserted_at_timestamp_type` - ...

    * `:updated_at` - ...

    * `:updated_at_source` - ...

    * `:updated_at_timestamp_type` - ...

    * `:timestamp_type` - ...

    * `:validate` - ...
  """
  def convert_to_insert_all_params(query, params_list, opts) do
    utc_now = datetime_utc_now()

    EctoShorts.Utils.reduce_all(params_list, fn arg ->
      changeset = build_changeset(arg, query)

      if has_primary_key?(query, changeset) do
        with {:ok, schema_data} <- Changeset.apply_action(changeset, :update) do
          {:ok,
           serialize_insert(
             query,
             schema_data,
             Map.keys(changeset.changes),
             utc_now,
             opts
           )}
        end
      else
        with {:ok, schema_data} <- Changeset.apply_action(changeset, :insert) do
          {:ok,
           serialize_insert(
             query,
             schema_data,
             Map.keys(changeset.changes),
             utc_now,
             opts
           )}
        end
      end
    end)
  end

  @doc """
  ...
  """
  def any_has_primary_key?(query, inserts) do
    Enum.any?(inserts, &has_primary_key?(query, &1))
  end

  @doc """
  ...
  """
  def has_primary_key?(query, %{data: %{__meta__: _} = schema_data}) do
    has_primary_key?(query, schema_data)
  end

  def has_primary_key?(query, data) do
    query
    |> CommonSchemas.get_schema_reflection(:primary_key)
    |> Enum.all?(fn key ->
      (Map.has_key?(data, key) and !nil_value?(data, key)) or
        (Map.has_key?(data, to_string(key)) and !nil_value?(data, to_string(key)))
    end)
  end

  @doc """
  ...
  """
  def build_upsert_options(opts, query) do
    [
      conflict_target: CommonSchemas.get_schema_reflection(query, :primary_key),
      on_conflict: {:replace, schema_replace_keys(query, opts)}
    ]
  end

  defp schema_replace_keys(query, opts) do
    query
    |> CommonSchemas.get_schema_reflection(:query_fields)
    |> Kernel.--(CommonSchemas.get_schema_reflection(query, :primary_key))
    |> Kernel.--([inserted_at_source(opts)])
  end

  defp serialize_insert(query, data, changed_keys, utc_now, opts) do
    data
    |> Map.take(CommonSchemas.get_schema_reflection(query, :query_fields))
    |> drop_nil_if_not_changed(changed_keys)
    |> maybe_put_placeholders(opts[:placeholders] || %{}, opts)
    |> put_timestamps(utc_now, query, opts)
  end

  defp build_changeset({%{data: %{__meta__: _}} = changeset, params}, query) do
    CommonSchemas.get_schema_queryable(query).changeset(changeset, params)
  end

  defp build_changeset({%{__meta__: _} = schema_data, params}, query) do
    CommonSchemas.get_schema_queryable(query).changeset(schema_data, params)
  end

  defp build_changeset(%{data: %{__meta__: _}} = changeset, query) do
    CommonSchemas.get_schema_queryable(query).changeset(changeset, %{})
  end

  defp build_changeset(%{__meta__: _} = schema_data, query) do
    CommonSchemas.get_schema_queryable(query).changeset(schema_data, %{})
  end

  defp build_changeset(params, query) do
    attrs =
      if has_primary_key?(query, params) do
        Map.take(params, CommonSchemas.get_schema_reflection(query, :primary_key))
      else
        %{}
      end

    query
    |> CommonSchemas.get_schema_queryable()
    |> struct!(attrs)
    |> CommonSchemas.get_schema_queryable(query).changeset(params)
  end

  defp drop_nil_if_not_changed(data, changed_keys) do
    data
    |> Enum.reject(fn {key, val} -> is_nil(val) and key not in changed_keys end)
    |> Map.new()
  end

  defp maybe_put_placeholders(data, placeholders, opts) do
    Enum.reduce(placeholders, data, &maybe_put_placeholder(&1, &2, opts))
  end

  defp maybe_put_placeholder({key, placeholder}, data, opts) do
    case Map.get(data, key) do
      nil ->
        put_placeholder(data, key)

      existing_value ->
        if existing_value === placeholder do
          put_placeholder(data, key)
        else
          on_placeholder_conflict(data, key, opts)
        end
    end
  end

  defp on_placeholder_conflict(data, key, opts) do
    case Keyword.get(opts, :on_placeholder_conflict, :nothing) do
      {:replace, keys} -> if key in keys, do: put_placeholder(data, key), else: data
      :replace_all -> put_placeholder(data, key)
      :nothing -> data
    end
  end

  defp put_placeholder(data, key), do: Map.put(data, key, {:placeholder, key})

  defp put_timestamps(data, datetime, query, opts) do
    data
    |> maybe_put_inserted_at(datetime, query, opts)
    |> put_timestamp_updated_at(datetime, query, opts)
  end

  defp maybe_put_inserted_at(data, datetime, query, opts) do
    inserted_at_source = inserted_at_source(opts)

    if inserted_at_source === false do
      data
    else
      case Map.get(data, inserted_at_source) do
        nil ->
          Map.put(
            data,
            inserted_at_source,
            dump_timestamp_inserted_at(datetime, inserted_at_source, query, opts)
          )

        _ ->
          data
      end
    end
  end

  defp put_timestamp_updated_at(data, datetime, query, opts) do
    updated_at_source = updated_at_source(opts)

    if updated_at_source === false do
      data
    else
      Map.put(
        data,
        updated_at_source,
        dump_timestamp_updated_at(datetime, updated_at_source, query, opts)
      )
    end
  end

  defp dump_timestamp_inserted_at(datetime, inserted_at_source, query, opts) do
    datetime
    |> maybe_datetime_to_naive(timestamp_type(opts, :inserted_at, inserted_at_source, query))
    |> truncate_datetime()
  end

  defp dump_timestamp_updated_at(datetime, updated_at_source, query, opts) do
    datetime
    |> maybe_datetime_to_naive(timestamp_type(opts, :updated_at, updated_at_source, query))
    |> truncate_datetime()
  end

  defp inserted_at_source(opts) do
    opts[:inserted_at] || opts[:inserted_at_source] || @inserted_at
  end

  defp updated_at_source(opts) do
    opts[:updated_at] || opts[:updated_at_source] || @updated_at
  end

  defp timestamp_type(opts, key, type_source, query) do
    opts[:timestamps][key] ||
      opts[:timestamp_type] ||
      CommonSchemas.get_schema_reflection(query, :type, type_source) ||
      @utc_datetime
  end

  defp datetime_utc_now, do: DateTime.utc_now()

  defp truncate_datetime(datetime) when is_struct(datetime, DateTime) do
    DateTime.truncate(datetime, :second)
  end

  defp truncate_datetime(naive_datetime) when is_struct(naive_datetime, NaiveDateTime) do
    NaiveDateTime.truncate(naive_datetime, :second)
  end

  defp maybe_datetime_to_naive(datetime, @naive_datetime), do: DateTime.to_naive(datetime)
  defp maybe_datetime_to_naive(datetime, @utc_datetime), do: datetime

  defp nil_value?(data, key) do
    data |> Map.get(key) |> is_nil()
  end
end
