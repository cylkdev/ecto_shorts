defmodule EctoShorts.Actions.CRUD do
  alias EctoShorts.{
    Actions.Error,
    Config,
    CommonFilters,
    CommonSchema
  }

  @doc group: "CRUD"
  @doc since: "2.5.0"
  @doc """
  ...
  """
  def exists?(source, params, opts \\ []) do
    source
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).exists?(opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def all(queryable) do
    all(queryable, %{}, [])
  end

  def all(queryable, params) when is_map(params) do
    all(queryable, params, [])
  end

  def all(queryable, opts) do
    params =
      opts
      |> Keyword.drop([:repo, :replica])
      |> Map.new()

    all(queryable, params, Keyword.take(opts, [:repo, :replica]))
  end

  def all(queryable, params, opts) do
    params =
      params
      |> put_order_by(opts)
      |> put_group_by(opts)

    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).all(opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def create(schema, params, opts \\ []) do
    schema
    |> CommonSchema.create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def get(queryable, id, opts \\ []) do
    Config.replica!(opts).get(queryable, id, opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def find(queryable, params, opts \\ [])

  def find(queryable, params, opts)
      when params === %{} and not is_struct(queryable, Ecto.Query) do
    {:error,
     Error.call(
       :not_found,
       "record not found.",
       %{
         query: queryable,
         params: params
       },
       opts
     )}
  end

  def find(queryable, params, opts) do
    params =
      params
      |> put_order_by(opts)
      |> put_group_by(opts)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    case queryable
         |> CommonFilters.convert_params_to_filter(params, opts)
         |> Config.replica!(opts).one(opts) do
      nil ->
        {:error,
         Error.call(
           :not_found,
           "record not found.",
           %{
             query: queryable,
             params: params
           },
           opts
         )}

      record ->
        {:ok, record}
    end
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def update(queryable, id_or_schema_data, params, opts \\ [])

  def update(queryable, id, params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, record} <- find(queryable, %{id: id}, opts) do
      update(queryable, record, params, opts)
    end
  end

  def update(queryable, schema_data, params, opts) do
    queryable
    |> CommonSchema.create_changeset(schema_data, params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def delete(data, opts \\ [])

  def delete(queryable, id) when is_binary(id) or is_integer(id) do
    delete(queryable, id, [])
  end

  def delete(%{data: %{__meta__: %{schema: schema}}} = changeset, opts) do
    with {:error, changeset} <-
           schema
           |> CommonSchema.create_changeset(changeset, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "failed to delete record.",
         %{
           schema: schema,
           changeset: changeset
         },
         opts
       )}
    end
  end

  def delete(%{__meta__: %{schema: schema}} = schema_data, opts) do
    with {:error, changeset} <-
           schema
           |> CommonSchema.create_changeset(schema_data, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "failed to delete record.",
         %{
           schema: schema,
           changeset: changeset
         },
         opts
       )}
    end
  end

  def delete(structs_or_changesets, opts) when is_list(structs_or_changesets) do
    with {:ok, results} <-
           Enum.reduce_while(structs_or_changesets, {:ok, []}, fn entry, {:ok, acc} ->
             case delete(entry, opts) do
               {:ok, result} -> {:cont, {:ok, [result | acc]}}
               {:error, reason} -> {:halt, {:error, reason}}
             end
           end) do
      {:ok, Enum.reverse(results)}
    end
  end

  def delete(queryable, id, opts) when is_integer(id) or is_binary(id) do
    with {:ok, record} <- find(queryable, %{id: id}, opts) do
      delete(record, opts)
    end
  end

  def delete(_queryable, %_{} = struct_or_changeset, opts) do
    delete(struct_or_changeset, opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def stream(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def aggregate(queryable, params \\ %{}, aggregate \\ :count, key \\ :id, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, key, opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def find_and_create(queryable, find_params, create_params, opts \\ []) do
    with {:error, _} <- find(queryable, find_params, opts) do
      create(queryable, create_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def find_and_update(source, find_params, update_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      update(source, record, update_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def find_and_upsert(source, find_params, upsert_params, opts \\ []) do
    case find(source, find_params, opts) do
      {:ok, record} -> update(source, record, upsert_params, opts)
      {:error, _} -> create(source, Map.merge(find_params, upsert_params), opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def find_and_delete(source, find_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      delete(record, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def find_or_create(source, params, opts \\ []) do
    with {:error, _} <-
           find(
             source,
             Map.take(params, get_query_fields(opts, source)),
             opts
           ) do
      source
      |> CommonSchema.get_schema_source()
      |> create(params, opts)
    end
  end

  defp put_order_by(enum, opts) do
    case Keyword.get(opts, :order_by) do
      nil ->
        enum

      order_by ->
        if is_map(enum) do
          Map.put(enum, :order_by, order_by)
        else
          enum ++ [order_by: order_by]
        end
    end
  end

  defp put_group_by(enum, opts) do
    case Keyword.get(opts, :group_by) do
      nil ->
        enum

      group_by ->
        if is_map(enum) do
          Map.put(enum, :group_by, group_by)
        else
          enum ++ [group_by: group_by]
        end
    end
  end

  defp get_query_fields(opts, source) do
    Keyword.get(opts, :query_fields, CommonSchema.get_schema_reflection(source, :query_fields))
  end
end
