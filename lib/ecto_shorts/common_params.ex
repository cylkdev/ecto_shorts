defmodule EctoShorts.CommonParams do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides helper functions for preparing data used with the
  Ecto repo functions `update_all` and `insert_all`.

  This module is responsible for taking application-level data
  and transforming it into the structures required by Ecto for
  performing bulk operations. It offers a simple interface for
  validating records, generating timestamps, handling placeholder
  values, and preparing conflict resolution behavior.

  It works by normalizing params data (such as maps or structs),
  optionally validating it through the schema’s `changeset/2`,
  and then generating the appropriate keyword lists or maps used
  by Ecto’s insert and update functions. If configured, it also
  automatically manages timestamps for `inserted_at` and
  `updated_at`, and handles scenarios where placeholder values
  need to be substituted during insert operations.

  This is especially useful when performing batch inserts or
  updates, ensuring consistent timestamp handling, optional
  schema validation, and support for placeholder values across
  your application.
  """

  alias Ecto.Changeset
  alias EctoShorts.CommonSchema
  alias EctoShorts.Utils

  @utc_datetime :utc_datetime
  @naive_datetime :naive_datetime

  @inserted_at :inserted_at
  @updated_at :updated_at

  @doc """
  ...
  """
  def build_on_conflict_options(source, inserts, opts) when is_list(inserts) do
    with schema when not is_nil(schema) <- normalize_schema(source),
         true <- inserts != [],
         true <- Enum.any?(inserts, &has_all_non_nil_primary_keys?(schema, &1)) do
      build_conflict_options(schema, inserts, opts)
    else
      _ -> []
    end
  end

  defp build_conflict_options(schema, inserts, opts) do
    conflict_target = schema.__schema__(:primary_key)
    replace_fields = get_replace_fields(inserts, conflict_target, opts)

    if replace_fields == [] do
      [conflict_target: conflict_target]
    else
      [conflict_target: conflict_target, on_conflict: {:replace, replace_fields}]
    end
  end

  defp get_replace_fields(inserts, conflict_target, opts) do
    case Keyword.get(opts, :on_conflict_replace, :insert_keys) do
      :none ->
        []

      :insert_keys ->
        inserts
        |> Enum.flat_map(&Map.keys/1)
        |> Enum.uniq()
        |> Enum.reject(&(&1 in conflict_target))
        |> Enum.sort()

      fields when is_list(fields) ->
        fields
        |> Enum.uniq()
        |> Enum.reject(&(&1 in conflict_target))
        |> Enum.sort()

      invalid_term ->
        raise ArgumentError,
              "Expected :on_conflict_replace to be :none, :insert_keys, or a list of fields, " <>
                "got: #{inspect(invalid_term)}"
    end
  end

  @doc """
  Converts a list of parameters or structs into the format
  expected by [Ecto.Repo.insert_all](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert_all/3), with support for
  validation, timestamps, and placeholder substitution.

  ## Options

  ### Placeholder Options

  These control how certain field values are replaced with placeholders.
  Placeholders are useful when you want to defer resolution of the final
  value to a later process, or signal that a special condition applies.

    * `placeholders`: A map where the key is the field name (as an atom)
      and the value is the placeholder value to match against.
      If the field value matches, it will be replaced with a tuple like
      `{:placeholder, :field_name}`, where `:field_name` refers to the
      actual name of the field being substituted.

    * `on_placeholder_conflict`: Controls what happens when a placeholder
      value is provided but the record already contains a different value.
      You can choose from:

        * `nothing` – Keep the existing value unchanged.

        * `replace_all` – Always use the placeholder regardless of conflict.

        * `{replace, fields}` – Only replace fields listed in the provided list.

  ### Timestamp Options

  These let you configure how inserted and updated timestamps are applied.

    * `inserted_at`, `updated_at`: Manually set the values for each timestamp.

    * `inserted_at_source`, `updated_at_source`: Customize the field name
      (for example, use `created_on` instead of `inserted_at`).

    * `inserted_at_timestamp_type`, `updated_at_timestamp_type`: Override
      the timestamp format type (e.g. naive or UTC).

    * `timestamp_type`: A fallback type for both inserted and updated fields.

  ### Validation Options

    * `validate`: If `true`, each entry is passed through the schema’s
      `changeset/2` function for validation. If `false`, raw structs are
      constructed without validation.
  """
  def convert_to_insert_params(source, params_list \\ [], opts \\ []) do
    schema = normalize_schema(source)

    case build_inserts(params_list, schema, opts) do
      {inserts, []} ->
        {:ok, Enum.reverse(inserts)}

      {_, errors} ->
        {:error, Enum.reverse(errors)}
    end
  end

  defp build_inserts(params_list, schema, opts) do
    utc_now = DateTime.utc_now()

    Enum.reduce(
      params_list,
      {[], []},
      fn params, {acc, errors} ->
        case normalize_insert_entry(schema, params, opts) do
          {:ok, insert_data, changed_keys} ->
            insert_data =
              build_insert_map(schema, insert_data, utc_now, changed_keys, opts)

            {[insert_data | acc], errors}

          {:error, e} ->
            {acc, [e | errors]}
        end
      end
    )
  end

  defp normalize_insert_params(nil, params, _opts) do
    to_map!(params)
  end

  defp normalize_insert_params(schema, params, opts) do
    query_fields = get_query_fields(opts, schema)

    params
    |> to_map!()
    |> Map.take(query_fields)
  end

  defp to_map!(params) do
    cond do
      is_map(params) ->
        Utils.atomize_keys(params)

      Keyword.keyword?(params) ->
        Map.new(params)

      true ->
        raise ArgumentError,
              "Expected params to be a map or keyword list, got: #{inspect(params)}"
    end
  end

  defp build_struct(schema, schema_struct, params, opts) do
    if opts[:validate] === false do
      {:ok, struct(schema_struct, params)}
    else
      schema_struct
      |> CommonSchema.create_changeset(params, opts)
      |> Changeset.apply_action(changeset_action(schema, schema_struct))
    end
  end

  defp build_changeset(schema, %{data: schema_struct} = changeset, opts) do
    if opts[:validate] === false do
      {:ok, Changeset.apply_changes(changeset)}
    else
      Changeset.apply_action(changeset, changeset_action(schema, schema_struct))
    end
  end

  defp build_schema_data(schema, params, opts) do
    if opts[:validate] === false do
      {:ok, struct(schema, params)}
    else
      with {:ok, insert_data} <-
             schema
             |> CommonSchema.create_changeset(params, opts)
             |> Changeset.apply_action(:insert) do
        insert_data =
          if has_all_non_nil_primary_keys?(schema, params) do
            struct!(insert_data, Map.take(params, schema.__schema__(:primary_key)))
          else
            insert_data
          end

        {:ok, insert_data}
      end
    end
  end

  defp normalize_insert_entry(schema, {%{data: schema_struct} = _changeset, params}, opts) do
    normalize_insert_entry(schema, {schema_struct, params}, opts)
  end

  defp normalize_insert_entry(nil, params, opts) do
    params = normalize_insert_params(nil, params, opts)

    cond do
      is_map(params) and not is_struct(params) ->
        {:ok, params, Map.keys(params)}

      true ->
        {:error, {:invalid_insert_entry, params}}
    end
  end

  defp normalize_insert_entry(schema, {%_{} = schema_struct, params}, opts) do
    params = normalize_insert_params(schema, params, opts)
    changed_keys = keys_changed_in_schema_data(schema_struct, params)

    with {:ok, insert_data} <- build_struct(schema, schema_struct, params, opts) do
      {:ok, insert_data, changed_keys}
    end
  end

  defp normalize_insert_entry(schema, %{data: schema_struct} = changeset, opts) do
    params = normalize_insert_params(schema, changeset.params, opts)
    changed_keys = keys_changed_in_schema_data(schema_struct, params)

    with {:ok, insert_data} <- build_changeset(schema, changeset, opts) do
      {:ok, insert_data, changed_keys}
    end
  end

  defp normalize_insert_entry(schema, %_{} = schema_struct, opts) do
    changed_keys = get_query_fields(opts, schema)

    with {:ok, insert_data} <- build_struct(schema, schema_struct, %{}, opts) do
      {:ok, insert_data, changed_keys}
    end
  end

  defp normalize_insert_entry(schema, params, opts) do
    params = normalize_insert_params(schema, params, opts)
    query_fields = get_query_fields(opts, schema)
    changed_keys = keys_changed_in_params(query_fields, params)

    with {:ok, insert_data} <- build_schema_data(schema, params, opts) do
      {:ok, insert_data, changed_keys}
    end
  end

  defp keys_changed_in_schema_data(schema_struct, map_b) do
    Enum.reduce(map_b, [], fn {key, val}, acc ->
      if Map.get(schema_struct, key) != val do
        [key | acc]
      else
        acc
      end
    end)
  end

  defp keys_changed_in_params(query_fields, params) do
    Enum.reduce(query_fields, [], fn query_field, acc ->
      if Map.has_key?(params, query_field) do
        [query_field | acc]
      else
        acc
      end
    end)
  end

  defp changeset_action(schema, schema_struct) do
    if has_all_non_nil_primary_keys?(schema, schema_struct) do
      :update
    else
      :insert
    end
  end

  defp has_all_non_nil_primary_keys?(schema, schema_data_or_params) do
    Enum.all?(schema.__schema__(:primary_key), fn key ->
      not is_nil(Map.get(schema_data_or_params, key))
    end)
  end

  defp build_insert_map(nil, insert_data, utc_now, changed_keys, opts) do
    insert_data
    |> filter_insert_changes(changed_keys)
    |> put_placeholders(opts[:placeholders] || %{}, opts)
    |> put_timestamps(utc_now, nil, opts)
  end

  defp build_insert_map(schema, insert_data, utc_now, changed_keys, opts) do
    query_fields = get_query_fields(opts, schema)

    insert_data
    |> Map.take(query_fields)
    |> filter_insert_changes(changed_keys)
    |> put_placeholders(opts[:placeholders] || %{}, opts)
    |> put_timestamps(utc_now, schema, opts)
  end

  defp filter_insert_changes(insert_data, changed_keys) do
    Enum.reduce(insert_data, %{}, fn {key, value}, acc ->
      if value !== nil or (value === nil and Enum.member?(changed_keys, key)) do
        Map.put(acc, key, value)
      else
        acc
      end
    end)
  end

  defp put_placeholders(data, placeholders, opts) do
    Enum.reduce(placeholders, data, &put_placeholder(&1, &2, opts))
  end

  defp put_placeholder({key, placeholder_value}, data, opts) do
    if Map.has_key?(data, key) do
      if Map.get(data, key) === placeholder_value do
        put_placeholder(data, key)
      else
        on_placeholder_conflict(data, key, opts)
      end
    else
      data
    end
  end

  defp on_placeholder_conflict(input, key, opts) do
    case Keyword.get(opts, :on_placeholder_conflict, :nothing) do
      {:replace, keys} ->
        if Enum.member?(keys, key) do
          put_placeholder(input, key)
        else
          input
        end

      :replace_all ->
        put_placeholder(input, key)

      :nothing ->
        input

      term ->
        raise ArgumentError,
              "Expected the value for option :on_placeholder_conflict to be one of " <>
                "[:replace, :replace_all, :nothing], got: #{inspect(term)}"
    end
  end

  defp put_placeholder(input, key), do: Map.put(input, key, {:placeholder, key})

  defp put_timestamps(input, datetime, schema, opts) do
    input
    |> maybe_put_inserted_at(datetime, schema, opts)
    |> put_timestamp_updated_at(datetime, schema, opts)
  end

  defp maybe_put_inserted_at(input, datetime, schema, opts) do
    source_key = inserted_at_source_key(opts)

    if source_key === false do
      input
    else
      result =
        if Map.has_key?(input, source_key) do
          case Map.get(input, source_key) do
            nil ->
              normalize_timestamp_inserted_at(datetime, source_key, schema, opts)

            existing_timestamp ->
              normalize_timestamp_inserted_at(existing_timestamp, source_key, schema, opts)
          end
        else
          normalize_timestamp_inserted_at(datetime, source_key, schema, opts)
        end

      Map.put(input, source_key, result)
    end
  end

  defp inserted_at_source_key(opts) do
    cond do
      Keyword.has_key?(opts, :inserted_at_source) -> opts[:inserted_at_source]
      true -> @inserted_at
    end
  end

  defp normalize_timestamp_inserted_at(datetime, inserted_at_source, schema, opts) do
    timestamp_type = timestamp_type(opts, :inserted_at, inserted_at_source, schema)

    datetime
    |> cast_datetime(timestamp_type)
    |> truncate_datetime()
  end

  defp put_timestamp_updated_at(input, datetime, schema, opts) do
    source_key = get_updated_at_source(opts)
    value = Keyword.get(opts, :updated_at)

    cond do
      source_key === false ->
        input

      value === false ->
        input

      true ->
        value = prepare_timestamp_updated_at(value || datetime, source_key, schema, opts)
        Map.put(input, source_key, value)
    end
  end

  @doc """
  Converts a map of update parameters into the format expected by
  [Ecto.Repo.update_all](https://hexdocs.pm/ecto/Ecto.Repo.html#c:update_all/3).

  This is useful when you need to perform a bulk update on a set of records.
  The parameters are grouped by action (for example, set, increment, push),
  and the result is returned in the format required by `update_all/3`.

  You can also choose to automatically update the `updated_at` field.

  ## Options

    * `updated_at` – Manually provide the timestamp value for the `updated_at` field.
    * `updated_at_source` – Change the field name that is used instead of `:updated_at`.
    * `updated_at_timestamp_type` – Override the format of the `updated_at` field
      (for example, UTC or naive datetime).
    * `timestamp_type` – Fallback timestamp format type if the above is not provided.
  """
  def convert_to_update_params(source, params, opts \\ []) do
    utc_now = DateTime.utc_now()

    schema = normalize_schema(source)

    with updates when updates !== [] <-
           schema
           |> build_update_operations(params, [], opts)
           |> group_update_operations() do
      updates
      |> put_set_updated_at(utc_now, schema, opts)
      |> Enum.map(fn {key, values} -> {key, Enum.sort(values)} end)
      |> Enum.sort()
    end
  end

  defp put_set_updated_at(updates, datetime, schema, opts) do
    source_key = get_updated_at_source(opts)
    value = Keyword.get(opts, :updated_at)

    cond do
      source_key === false ->
        updates

      value === false ->
        updates

      true ->
        value = prepare_timestamp_updated_at(value || datetime, source_key, schema, opts)

        Keyword.update(
          updates,
          :set,
          [{source_key, value}],
          &Keyword.put(&1, source_key, value)
        )
    end
  end

  defp group_update_operations(update_operations) do
    update_operations
    |> Enum.group_by(fn {op, _key, _value} -> op end)
    |> Enum.map(fn {op, updates} ->
      {op, Enum.map(updates, fn {_, key, value} -> {key, value} end)}
    end)
  end

  defp build_update_operations(source, %{} = params, acc, opts) do
    build_update_operations(source, Map.to_list(params), acc, opts)
  end

  defp build_update_operations(_source, [], acc, _opts) do
    acc
  end

  defp build_update_operations(source, [head | tail], acc, opts) do
    with acc <- build_update_operations(source, head, acc, opts) do
      build_update_operations(source, tail, acc, opts)
    end
  end

  defp build_update_operations(source, {key, value}, acc, opts) do
    case source do
      nil ->
        normalize_update_value(nil, key, value, acc)

      schema ->
        if key in get_query_fields(opts, schema) do
          normalize_update_value(schema, key, value, acc)
        else
          acc
        end
    end
  end

  defp normalize_update_value(source, key, value, acc) do
    cond do
      is_list(value) and
          Enum.all?(value, &match?({op, _val} when op in [:set, :inc, :push, :pull], &1)) ->
        Enum.reduce(value, acc, fn v, acc ->
          reduce_updates(source, key, v, acc)
        end)

      true ->
        reduce_updates(source, key, value, acc)
    end
  end

  defp reduce_updates(source, key, {op, value}, acc)
       when op in [:pull, :push] do
    case validate_field_type_of_array(source, key) do
      :ok ->
        value
        |> List.wrap()
        |> Enum.reduce(acc, fn value, acc ->
          [{op, key, value} | acc]
        end)

      {:error, actual_type} ->
        raise ArgumentError,
              """
              The field `#{inspect(key)}` on schema `#{inspect(source)}` is not a type of `:array`
              and cannot be used with the `Ecto.Query` update operator `#{inspect(op)}`.

              actual type:
              #{inspect(actual_type)}
              """
    end
  end

  defp reduce_updates(source, key, {:inc, value}, acc) do
    case validate_field_type_of_int(source, key) do
      :ok ->
        if is_integer(value) do
          [{:inc, key, value} | acc]
        else
          raise ArgumentError,
                "Expected value for key `#{inspect(key)}` to be an integer, got: #{inspect(value)}"
        end

      {:error, actual_type} ->
        raise ArgumentError,
              """
              The field `#{inspect(key)}` on schema `#{inspect(source)}` is not a type of `:integer`
              and cannot be used with the `Ecto.Query` update operator `:inc`.

              actual type:
              #{inspect(actual_type)}
              """
    end
  end

  defp reduce_updates(_source, key, {:set, value}, acc) do
    [{:set, key, value} | acc]
  end

  defp reduce_updates(_source, key, value, acc) do
    [{:set, key, value} | acc]
  end

  defp validate_field_type_of_int(source, key) do
    case source do
      nil ->
        :ok

      schema ->
        case schema.__schema__(:type, key) do
          :integer -> :ok
          val -> {:error, val}
        end
    end
  end

  defp validate_field_type_of_array(source, key) do
    case source do
      nil ->
        :ok

      schema ->
        case schema.__schema__(:type, key) do
          {:array, _} -> :ok
          val -> {:error, val}
        end
    end
  end

  # Helpers

  defp normalize_schema({_source, nil}), do: nil
  defp normalize_schema({_source, schema}) when is_atom(schema), do: schema
  defp normalize_schema(nil), do: nil
  defp normalize_schema(schema) when is_atom(schema), do: schema
  defp normalize_schema(_), do: nil

  defp get_updated_at_source(opts) do
    cond do
      Keyword.has_key?(opts, :updated_at_source) -> opts[:updated_at_source]
      true -> @updated_at
    end
  end

  defp cast_datetime(%NaiveDateTime{} = naive_datetime, _), do: naive_datetime
  defp cast_datetime(datetime, @naive_datetime), do: DateTime.to_naive(datetime)
  defp cast_datetime(datetime, @utc_datetime), do: datetime

  defp truncate_datetime(%DateTime{} = datetime), do: DateTime.truncate(datetime, :second)

  defp truncate_datetime(%NaiveDateTime{} = naive_datetime),
    do: NaiveDateTime.truncate(naive_datetime, :second)

  defp timestamp_type(opts, key, type_source, schema) do
    schema_timestamp_type =
      if not is_nil(schema) do
        schema.__schema__(:type, type_source)
      end

    opts[:"#{key}_timestamp_type"] ||
      Keyword.get(opts[:timestamps] || [], key) ||
      opts[:timestamp_type] ||
      schema_timestamp_type ||
      @utc_datetime
  end

  defp prepare_timestamp_updated_at(datetime, updated_at_source, schema, opts) do
    timestamp_type = timestamp_type(opts, :updated_at, updated_at_source, schema)

    datetime
    |> cast_datetime(timestamp_type)
    |> truncate_datetime()
  end

  defp get_query_fields(opts, source) do
    Keyword.get(opts, :query_fields, CommonSchema.get_schema_reflection(source, :query_fields))
  end
end
