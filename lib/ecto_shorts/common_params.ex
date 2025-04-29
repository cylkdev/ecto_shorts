defmodule EctoShorts.CommonParams do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  alias Ecto.Changeset
  alias EctoShorts.CommonSchemas

  @utc_datetime :utc_datetime
  @naive_datetime :naive_datetime

  @inserted_at :inserted_at
  @updated_at :updated_at

  @doc """
  ...
  """
  def convert_to_update_all_params(schema_module, params, opts \\ []) do
    utc_now = datetime_utc_now()

    params
    |> normalize_updates(schema_module)
    |> group_updates_by_action()
    |> flatten_updates()
    |> maybe_set_updated_at(utc_now, schema_module, opts)
  end

  defp maybe_set_updated_at(keyword, datetime, schema_module, opts) do
    updated_at_source = opts[:updated_at] || opts[:updated_at_source] || @updated_at

    datetime = dump_timestamp_updated_at(datetime, updated_at_source, schema_module, opts)

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

  defp normalize_updates(params, schema_module) do
    Enum.reduce(params, [], &normalize_update(&1, schema_module, &2))
  end

  defp normalize_update({field, params}, schema_module, acc)
       when is_list(params) or is_map(params) do
    if field in schema_module.__schema__(:query_fields) do
      params
      |> Enum.reduce(acc, fn {action, value}, acc ->
        [update_invoc(action, field, value) | acc]
      end)
      |> Enum.reverse()
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{schema_module}'."
      )

      acc
    end
  end

  defp normalize_update({field, {action, value}}, schema_module, acc) do
    if field in schema_module.__schema__(:query_fields) do
      [update_invoc(action, field, value) | acc]
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{schema_module}'."
      )

      acc
    end
  end

  defp normalize_update({field, value}, schema_module, acc) do
    if field in schema_module.__schema__(:query_fields) do
      [update_invoc(:set, field, value) | acc]
    else
      EctoShorts.Utils.Logger.warning(
        __MODULE__,
        "The field '#{inspect(field)}' is not a query field on the schema '#{schema_module}'."
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

  @default_insert_all_options [validate: true]

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
  def convert_to_insert_all_params(schema_module, params_list, opts \\ []) do
    opts = Keyword.merge(@default_insert_all_options, opts)

    utc_now = datetime_utc_now()

    EctoShorts.Utils.reduce_all(params_list, fn arg ->
      with {:ok, schema_data, changed_keys} <- apply_change(schema_module, arg, opts) do
        {:ok, serialize_insert(schema_module, schema_data, changed_keys, utc_now, opts)}
      end
    end)
  end

  defp apply_change(
         schema_module,
         {%{data: %{__meta__: _} = schema_data} = changeset, params},
         opts
       ) do
    if opts[:validate] do
      action = if has_primary_key?(schema_module, schema_data), do: :update, else: :insert

      changeset = schema_module.changeset(changeset, params)

      with {:ok, schema_data} <- Changeset.apply_action(changeset, action) do
        {:ok, schema_data, Map.keys(changeset.changes)}
      end
    else
      {:ok, struct(schema_data, params), get_changed_keys(schema_data, params)}
    end
  end

  defp apply_change(schema_module, {%{__meta__: _} = schema_data, params}, opts) do
    if opts[:validate] do
      action = if has_primary_key?(schema_module, schema_data), do: :update, else: :insert

      changeset = schema_module.changeset(schema_data, params)

      with {:ok, schema_data} <- Changeset.apply_action(changeset, action) do
        {:ok, schema_data, Map.keys(changeset.changes)}
      end
    else
      {:ok, struct(schema_data, params), get_changed_keys(schema_data, params)}
    end
  end

  defp apply_change(schema_module, %{data: %{__meta__: _} = schema_data} = changeset, opts) do
    if opts[:validate] do
      action = if has_primary_key?(schema_module, schema_data), do: :update, else: :insert

      changeset = schema_module.changeset(changeset, %{})

      with {:ok, schema_data} <- Changeset.apply_action(changeset, action) do
        {:ok, schema_data, Map.keys(changeset.changes)}
      end
    else
      changed_keys =
        schema_data
        |> EctoShorts.SchemaHelpers.struct_to_jsonable_map()
        |> Map.keys()

      {:ok, schema_data, changed_keys}
    end
  end

  defp apply_change(schema_module, %{__meta__: _} = schema_data, opts) do
    if opts[:validate] do
      action = if has_primary_key?(schema_module, schema_data), do: :update, else: :insert

      changeset = schema_module.changeset(schema_data, %{})

      with {:ok, schema_data} <- Changeset.apply_action(changeset, action) do
        {:ok, schema_data, Map.keys(changeset.changes)}
      end
    else
      changed_keys =
        schema_module
        |> struct()
        |> EctoShorts.SchemaHelpers.struct_to_jsonable_map()
        |> Map.keys()

      {:ok, schema_data, changed_keys}
    end
  end

  defp apply_change(schema_module, params, opts) do
    if opts[:validate] do
      action = if has_primary_key?(schema_module, params), do: :update, else: :insert

      attrs =
        if has_primary_key?(schema_module, params) do
          Map.take(params, CommonSchemas.get_schema_reflection(schema_module, :primary_key))
        else
          %{}
        end

      changeset =
        schema_module
        |> struct!(attrs)
        |> schema_module.changeset(params)

      with {:ok, schema_data} <- Changeset.apply_action(changeset, action) do
        {:ok, schema_data, Map.keys(changeset.changes)}
      end
    else
      schema_data = struct(schema_module, params)

      {:ok, schema_data, Map.keys(params)}
    end
  end

  defp get_changed_keys(schema_data, params) do
    Enum.reduce(params, [], fn {key, value}, acc ->
      if Map.get(schema_data, key) !== value do
        [key | acc]
      else
        acc
      end
    end)
  end

  defp serialize_insert(schema_module, data, changed_keys, utc_now, opts) do
    data
    |> Map.take(schema_module.__schema__(:query_fields))
    |> drop_nil_if_not_changed(changed_keys)
    |> reduce_placeholders(opts[:placeholders] || %{}, opts)
    |> put_timestamps(utc_now, schema_module, opts)
  end

  @doc """
  ...
  """
  def any_has_primary_key?(schema_module, inserts) do
    Enum.any?(inserts, &has_primary_key?(schema_module, &1))
  end

  @doc """
  ...
  """
  def has_primary_key?(schema_module, %{data: %{__meta__: _} = schema_data}) do
    has_primary_key?(schema_module, schema_data)
  end

  def has_primary_key?(schema_module, data) do
    Enum.all?(schema_module.__schema__(:primary_key), fn key ->
      (Map.has_key?(data, key) and !nil_value?(data, key)) or
        (Map.has_key?(data, to_string(key)) and !nil_value?(data, to_string(key)))
    end)
  end

  @doc """
  ...
  """
  def on_conflict_options(opts, schema_module) do
    [
      conflict_target: schema_module.__schema__(:primary_key),
      on_conflict: {:replace, schema_replace_keys(schema_module, opts)}
    ]
  end

  defp schema_replace_keys(schema_module, opts) do
    schema_module.__schema__(:query_fields)
    |> Kernel.--(schema_module.__schema__(:primary_key))
    |> Kernel.--([inserted_at_source(opts)])
  end

  defp drop_nil_if_not_changed(data, changed_keys) do
    data
    |> Enum.reject(fn {key, val} -> is_nil(val) and key not in changed_keys end)
    |> Map.new()
  end

  defp reduce_placeholders(data, placeholders, opts) do
    Enum.reduce(placeholders, data, &reduce_placeholder(&1, &2, opts))
  end

  defp reduce_placeholder({key, placeholder}, data, opts) do
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

  defp put_timestamps(data, datetime, schema_module, opts) do
    data
    |> maybe_put_inserted_at(datetime, schema_module, opts)
    |> put_timestamp_updated_at(datetime, schema_module, opts)
  end

  defp maybe_put_inserted_at(data, datetime, schema_module, opts) do
    inserted_at_source = inserted_at_source(opts)

    if inserted_at_source === false do
      data
    else
      case Map.get(data, inserted_at_source) do
        nil ->
          Map.put(
            data,
            inserted_at_source,
            dump_timestamp_inserted_at(datetime, inserted_at_source, schema_module, opts)
          )

        _ ->
          data
      end
    end
  end

  defp put_timestamp_updated_at(data, datetime, schema_module, opts) do
    updated_at_source = updated_at_source(opts)

    if updated_at_source === false do
      data
    else
      Map.put(
        data,
        updated_at_source,
        dump_timestamp_updated_at(datetime, updated_at_source, schema_module, opts)
      )
    end
  end

  defp dump_timestamp_inserted_at(datetime, inserted_at_source, schema_module, opts) do
    datetime
    |> maybe_datetime_to_naive(
      timestamp_type(opts, :inserted_at, inserted_at_source, schema_module)
    )
    |> truncate_datetime()
  end

  defp dump_timestamp_updated_at(datetime, updated_at_source, schema_module, opts) do
    datetime
    |> maybe_datetime_to_naive(
      timestamp_type(opts, :updated_at, updated_at_source, schema_module)
    )
    |> truncate_datetime()
  end

  defp inserted_at_source(opts) do
    opts[:inserted_at] || opts[:inserted_at_source] || @inserted_at
  end

  defp updated_at_source(opts) do
    opts[:updated_at] || opts[:updated_at_source] || @updated_at
  end

  defp timestamp_type(opts, key, type_source, schema_module) do
    opts[:timestamps][key] ||
      opts[:timestamp_type] ||
      schema_module.__schema__(:type, type_source) ||
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
