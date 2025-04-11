defmodule EctoShorts.CommonParams do
  @moduledoc false
  alias Ecto.Changeset
  alias EctoShorts.CommonSchemas

  @utc_datetime :utc_datetime
  @naive_datetime :naive_datetime

  def convert_to_update_all_params(query, params, opts \\ []) do
    utc_now = datetime_utc_now()

    params
    |> normalize_updates(query)
    |> group_updates_by_action()
    |> flatten_updates()
    |> maybe_set_updated_at(utc_now, query, opts)
  end

  defp maybe_set_updated_at(keyword, datetime, query, opts) do
    updated_at_alias = opts[:updated_at] || opts[:updated_at_source]

    datetime = dump_updated_at_timestamp(datetime, updated_at_alias, query, opts)

    Keyword.update(
      keyword,
      :set,
      [{updated_at_alias, datetime}],
      &Keyword.put(&1, updated_at_alias, datetime)
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
        [update_action_field_value(action, field, value) | acc]
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
      [update_action_field_value(action, field, value) | acc]
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
      [update_action_field_value(:set, field, value) | acc]
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{CommonSchemas.get_schema_queryable(query)}'."
      )

      acc
    end
  end

  defp update_action_field_value(:inc, field, value) when is_integer(value),
    do: {:inc, field, value}

  defp update_action_field_value(:push, field, value) when is_list(value),
    do: {:push, field, value}

  defp update_action_field_value(:pull, field, value) when is_list(value),
    do: {:pull, field, value}

  defp update_action_field_value(:set, field, value), do: {:set, field, value}

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

    EctoShorts.Utils.reduce_all(params_list, fn params ->
      with {:ok, struct} <- apply_change(query, params, opts) do
        {
          :ok,
          struct
          |> Ecto.embedded_dump(:json)
          |> maybe_put_placeholders(opts[:placeholders] || %{}, opts)
          |> put_timestamps(utc_now, query, opts)
        }
      end
    end)
  end

  defp apply_change(query, {%_{data: %_{__meta__: _}} = changeset, params}, _opts) do
    changeset
    |> CommonSchemas.get_schema_queryable(query).changeset(params)
    |> Changeset.apply_action(:update)
  end

  defp apply_change(query, {%_{__meta__: _} = struct, params}, opts) do
    if opts[:validate] === false do
      {:ok, struct!(struct, params)}
    else
      struct
      |> CommonSchemas.get_schema_queryable(query).changeset(params)
      |> Changeset.apply_action(:update)
    end
  end

  defp apply_change(query, %_{data: %_{__meta__: _}} = changeset, _opts) do
    changeset
    |> CommonSchemas.get_schema_queryable(query).changeset(%{})
    |> Changeset.apply_action(:update)
  end

  defp apply_change(query, %_{__meta__: %{schema: schema_module}} = struct, opts)
       when query === schema_module do
    if opts[:validate] === false do
      {:ok, struct!(struct, %{})}
    else
      struct
      |> CommonSchemas.get_schema_queryable(query).changeset(%{})
      |> Changeset.apply_action(:update)
    end
  end

  defp apply_change(query, params, opts) when is_map(params) and not is_struct(params) do
    if opts[:validate] === false do
      {:ok, struct!(query, params)}
    else
      params
      |> create_changeset(query)
      |> Changeset.apply_action(:insert)
    end
  end

  defp apply_change(query, term, _opts) do
    raise ArgumentError, """
    Failed to apply change.

    Expected params to be one of:

    - { changeset, map }
    - { struct, map }
    - changeset
    - struct
    - map

    got:

    #{inspect(term)}

    query:

    #{inspect(query)}
    """
  end

  defp create_changeset(params, query) do
    queryable = CommonSchemas.get_schema_queryable(query)

    queryable
    |> struct!(%{})
    |> queryable.changeset(params)
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
      :nothing ->
        data

      {:replace, keys} ->
        if key in keys do
          put_placeholder(data, key)
        else
          data
        end

      :replace_all ->
        put_placeholder(data, key)
    end
  end

  defp put_placeholder(data, key), do: Map.put(data, key, {:placeholder, key})

  defp put_timestamps(data, datetime, query, opts) do
    data
    |> maybe_put_inserted_at(datetime, query, opts)
    |> put_timestamp_updated_at(datetime, query, opts)
  end

  defp maybe_put_inserted_at(data, datetime, query, opts) do
    if !is_nil(data[:id]) do
      data
    else
      inserted_at_alias = opts[:inserted_at] || opts[:inserted_at_source]

      if inserted_at_alias === false do
        data
      else
        case Map.get(data, inserted_at_alias) do
          nil ->
            Map.put(
              data,
              inserted_at_alias,
              dump_inserted_at_timestamp(datetime, inserted_at_alias, query, opts)
            )

          _ ->
            data
        end
      end
    end
  end

  defp put_timestamp_updated_at(data, datetime, query, opts) do
    updated_at_alias = opts[:updated_at] || opts[:updated_at_source]

    if updated_at_alias === false do
      data
    else
      Map.put(
        data,
        updated_at_alias,
        dump_updated_at_timestamp(datetime, updated_at_alias, query, opts)
      )
    end
  end

  defp dump_inserted_at_timestamp(datetime, inserted_at_alias, query, opts) do
    inserted_at_alias =
      if inserted_at_alias do
        inserted_at_alias
      else
        :inserted_at
      end

    timestamp_type =
      opts[:inserted_at_timestamp_type] ||
        opts[:timestamp_type] ||
        CommonSchemas.get_schema_queryable(query).__schema__(:type, inserted_at_alias) ||
        @utc_datetime

    datetime
    |> maybe_datetime_to_naive(timestamp_type)
    |> truncate_datetime()
  end

  defp dump_updated_at_timestamp(datetime, updated_at_alias, query, opts) do
    updated_at_alias =
      if updated_at_alias do
        updated_at_alias
      else
        :updated_at
      end

    timestamp_type =
      opts[:updated_at_timestamp_type] ||
        opts[:timestamp_type] ||
        CommonSchemas.get_schema_queryable(query).__schema__(:type, updated_at_alias) ||
        @utc_datetime

    datetime
    |> maybe_datetime_to_naive(timestamp_type)
    |> truncate_datetime()
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
end
