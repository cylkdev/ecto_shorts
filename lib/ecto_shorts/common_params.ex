defmodule EctoShorts.CommonParams do
  @moduledoc """
  ...
  """
  @moduledoc since: "2.5.0"

  alias EctoShorts.{
    CommonBatches,
    CommonSchemas
  }

  @logger_prefix "EctoShorts.CommonParams"

  @utc_datetime :utc_datetime

  @inserted_at_field_name :inserted_at
  @updated_at_field_name :updated_at

  @time_zone_utc "Etc/UTC"

  ## Insert API

  @doc """
  ...
  """
  @doc since: "2.5.0"
  def convert_to_insert_all_params(query, params_list, opts \\ []) do
    queryable = CommonSchemas.get_schema_queryable(query)

    naive_datetime = maybe_generate_naive_datetime(opts)

    field = opts[:preload] || :id
    query_params = opts[:preload_filter] || %{}

    results_errors =
      queryable
      |> batch_preload(params_list, field, query_params, opts)
      |> Stream.with_index()
      |> Enum.reduce({[], []}, fn {params, idx}, {results, errors} ->
        case apply_insert_change(queryable, params, naive_datetime, opts) do
          {:ok, res} -> {[res | results], errors}
          {:error, err} -> {results, [{idx, err} | errors]}
        end
      end)

    case results_errors do
      {results, []} -> {:ok, Enum.reverse(results)}
      {_, errors} -> {:error, Enum.reverse(errors)}
    end
  end

  ## Data validation

  defp apply_insert_change(queryable, {changeset, params}, naive_datetime, opts) when is_struct(changeset, Ecto.Changeset) do
    action = if has_id?(changeset.data), do: :update, else: :insert

    params = drop_associations(params, queryable)

    if Keyword.get(opts, :validate, true) do
      with {:ok, struct} <-
        queryable
        |> CommonSchemas.prepare_changeset(changeset, params, opts)
        |> Ecto.Changeset.apply_action(opts[:action] || changeset.action || action) do

        {:ok, dump_insert_change(struct, queryable, naive_datetime, opts)}
      end
    else
      result =
        changeset.data
        |> struct!(Map.merge(changeset.changes, params))
        |> dump_insert_change(queryable, naive_datetime, opts)

      {:ok, result}
    end
  end

  defp apply_insert_change(queryable, {%_{} = struct, params}, naive_datetime, opts) do
    action = if has_id?(struct), do: :update, else: :insert

    params = drop_associations(params, queryable)

    if Keyword.get(opts, :validate, true) do
      with {:ok, struct} <-
        queryable
        |> CommonSchemas.prepare_changeset(struct, params, opts)
        |> Ecto.Changeset.apply_action(opts[:action] || action) do

        {:ok, dump_insert_change(struct, queryable, naive_datetime, opts)}
      end
    else
      result =
        struct
        |> struct!(params)
        |> dump_insert_change(queryable, naive_datetime, opts)

      {:ok, result}
    end
  end

  defp apply_insert_change(queryable, changeset, naive_datetime, opts) when is_struct(changeset, Ecto.Changeset) do
    apply_insert_change(queryable, {changeset, %{}}, naive_datetime, opts)
  end

  defp apply_insert_change(queryable, %_{} = struct, naive_datetime, opts) do
    apply_insert_change(queryable, {struct, %{}}, naive_datetime, opts)
  end

  defp apply_insert_change(queryable, params, naive_datetime, opts) do
    action = if has_id?(params), do: :update, else: :insert

    params = drop_associations(params, queryable)

    if Keyword.get(opts, :validate, true) do
      with {:ok, struct} <-
        queryable
        |> CommonSchemas.prepare_changeset(struct(queryable), params, opts)
        |> Ecto.Changeset.apply_action(opts[:action] || action) do

        {:ok, dump_insert_change(struct, queryable, naive_datetime, opts)}
      end
    else
      result =
        queryable
        |> struct!(params)
        |> dump_insert_change(queryable, naive_datetime, opts)

      {:ok, result}
    end
  end

  defp dump_insert_change(struct, queryable, datetime, opts) do
    struct
    |> schema_to_map()
    |> drop_nil_values()
    |> drop_associations(queryable)
    |> maybe_put_placeholders(opts)
    |> put_timestamp_inserted_at(queryable, datetime, opts)
    |> put_timestamp_updated_at(queryable, datetime, opts)
  end

  ## Placeholders

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

  ## Timestamps

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

  @doc """
  ...
  """
  @doc since: "2.5.0"
  def batch_preload(queryable, params_list, field, query_params, opts) do
    # TODO: add support for many fields

    {batch_values, batch_value_idx_list} = split_preload_batch_values(params_list, field, opts)

    if Enum.any?(batch_values) do
      EctoShorts.Utils.Logger.debug(
        @logger_prefix,
        "Preload | fetching batch values | field=#{inspect(field)}, schema=#{inspect(queryable)}, values=#{inspect(batch_values, charlists: false)}"
      )

      queryable
      |> CommonBatches.batch_all(field, batch_values, query_params, :set, opts)
      |> merge_batch_structs(params_list, batch_value_idx_list)
    else
      params_list
    end
  end

  defp merge_batch_structs(batch_value_structs, params_list, batch_value_idx_list) do
    Enum.reduce(batch_value_structs, params_list, fn
      {batch_value, new_struct}, acc ->
        idx_list = Map.fetch!(batch_value_idx_list, batch_value)

        Enum.reduce(idx_list, acc, fn idx, acc ->
          case Enum.at(params_list, idx) do
            {%{data: %{__meta__: _} = changeset}, params} = current ->
              next = {%{changeset | data: new_struct}, params}

              EctoShorts.Utils.Logger.debug(
                @logger_prefix,
                """
                Preload | merging result | index=#{idx}, value=#{batch_value}

                current:
                #{inspect(current)}

                next:
                #{inspect(next, pretty: true)}
                """
              )

              put_in(acc, [Access.at(idx)], next)

            {_struct, params} = current ->
              next = {new_struct, params}

              EctoShorts.Utils.Logger.debug(
                @logger_prefix,
                """
                Preload | merging result | index=#{idx}, value=#{batch_value}

                current:
                #{inspect(current)}

                next:
                #{inspect(next, pretty: true)}
                """
              )

              put_in(acc, [Access.at(idx)], next)

            params ->
              next = {new_struct, params}

              EctoShorts.Utils.Logger.debug(
                @logger_prefix,
                """
                Preload | merging result | index=#{idx}, value=#{batch_value}

                current:
                #{inspect(params)}

                next:
                #{inspect(next, pretty: true)}
                """
              )

              put_in(acc, [Access.at(idx)], next)

          end
        end)
    end)
  end

  defp split_preload_batch_values(params_list, key, opts) do
    result =
      params_list
      |> Stream.with_index()
      |> Enum.reduce({[], %{}}, fn
        {{%{data: %{__meta__: _ = struct} = _changeset}, _params}, idx}, {values, batch_value_idx_list} = acc ->
          if opts[:force_preload] do
            case Map.fetch!(struct, key) do
              nil -> acc
              val ->
                EctoShorts.Utils.Logger.debug(
                  @logger_prefix,
                  "Queueing preload from existing data | index=#{idx}, key=#{key}, value=#{val}"
                )

                {[val | values], Map.update(batch_value_idx_list, val, [idx], &[&1 | idx])}
            end
          else
            acc
          end

        {{struct, _params}, idx}, {values, batch_value_idx_list} = acc ->
          if opts[:force_preload] do
            case Map.fetch!(struct, key) do
              nil -> acc
              val ->
                EctoShorts.Utils.Logger.debug(
                  @logger_prefix,
                  "Queueing preload from existing data | index=#{idx}, key=#{key}, value=#{val}"
                )

                {[val | values], Map.update(batch_value_idx_list, val, [idx], &[&1 | idx])}
            end
          else
            acc
          end

        {params, idx}, {values, batch_value_idx_list} = acc ->
          case Map.get(params, key) || Map.get(params, Atom.to_string(key)) do
            nil -> acc
            val ->
              EctoShorts.Utils.Logger.debug(
                @logger_prefix,
                "Queueing preload | index=#{idx}, key=#{key}, value=#{val}"
              )

              {[val | values], Map.update(batch_value_idx_list, val, [idx], &[&1 | idx])}
          end

      end)

    {values, batch_value_idx_list} = result

    {values |> Enum.uniq() |> Enum.reverse(), batch_value_idx_list}
  end

  ## Update API

  @doc """
  ...
  """
  @doc since: "2.5.0"
  @spec convert_to_update_all_params(
    query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
    params :: map() | keyword(),
    opts :: keyword()
  ) :: keyword()
  def convert_to_update_all_params(query, params, opts \\ [])

  def convert_to_update_all_params(query, params, opts) when is_map(params) do
    convert_to_update_all_params(query, Map.to_list(params), opts)
  end

  def convert_to_update_all_params(query, params, opts) do
    queryable = CommonSchemas.get_schema_queryable(query)

    naive_datetime = maybe_generate_naive_datetime(opts)

    params
    |> drop_id()
    |> drop_associations(queryable)
    |> normalize_updates()
    |> merge_updates(queryable)
    |> put_set_updated_at(queryable, naive_datetime, opts)
    |> maybe_sort_update_params(opts)
  end

  defp maybe_sort_update_params(params, opts) do
    if Keyword.get(opts, :ordered, true) do
      params
      |> Enum.map(fn {action, params} ->
        {action, Enum.sort_by(params, fn {key, _} -> key end)}
      end)
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

  ## Timestamps

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

  ## Helpers

  defp schema_to_map(struct) do
    Map.drop(struct, [:__meta__, :__struct__])
  end

  defp drop_id(params) do
    Enum.reject(params, fn {k, _} -> k in [:id, "id"] end)
  end

  defp drop_associations(params, queryable) when is_list(params) do
    Keyword.drop(params, queryable.__schema__(:associations))
  end

  defp drop_associations(params, queryable) do
    Map.drop(params, queryable.__schema__(:associations))
  end

  defp has_id?(params), do: Map.has_key?(params, :id) || Map.has_key?(params, "id")

  defp drop_nil_values(params) do
    params
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Map.new()
  end
end
