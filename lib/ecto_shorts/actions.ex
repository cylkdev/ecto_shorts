defmodule EctoShorts.Actions do
  @moduledoc """
  Public actions API for common CRUD, batch, bulk, multi, and transaction operations.
  """

  alias EctoShorts.Actions.Batch
  alias EctoShorts.Actions.Error
  alias EctoShorts.Actions.Multi

  alias EctoShorts.{
    Config,
    CommonFilters,
    CommonParams,
    CommonSchema
  }

  @cardinalities [:one, :many]

  @doc group: "CRUD"
  @doc since: "2.5.0"
  @doc """
  Preloads all associations on the given struct or structs.

  ## Options

  See [Ecto.Query.preload/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:preload/3) for more information.
  """
  def preload(data, preloads, opts \\ []) do
    Config.replica!(opts).preload(data, preloads, opts)
  end

  @doc group: "CRUD"
  @doc since: "2.5.0"
  @doc """
  Checks if there exists an entry that matches the query.

  ## Options

  See [Ecto.Query.exists?/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:exists?/2) for more information.
  """
  def exists?(source, params, opts \\ []) do
    source
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).exists?(opts)
  end

  @doc group: "CRUD"
  @doc """
  Short-hand function for `all/3` with empty params and opts.
  """
  def all(queryable) do
    all(queryable, %{}, [])
  end

  @doc group: "CRUD"
  @doc """
  Short-hand function for `all/3`.

  ## Examples

      iex> EctoShorts.Actions.all(Post, %{})
      iex> EctoShorts.Actions.all(Post, replica: MyApp.Repo.Replica)
  """
  def all(queryable, params) when is_map(params) do
    all(queryable, params, [])
  end

  def all(queryable, opts) do
    params =
      opts
      |> Keyword.drop([:repo, :replica, :dynamic_adapter])
      |> Map.new()

    all(queryable, params, Keyword.take(opts, [:repo, :replica, :dynamic_adapter]))
  end

  @doc group: "CRUD"
  @doc """
  Fetches all entries from the data store matching the given query.

  ## Options

  See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more information.
  """
  def all(queryable, params, opts) do
    params =
      params
      |> put_param(opts, :order_by)
      |> put_param(opts, :group_by)

    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).all(opts)
  end

  @doc group: "CRUD"
  @doc """
  Creates a new record.

  ## Options

  See [Ecto.Repo.insert/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) for more information.
  """
  def create(schema, params, opts \\ []) do
    schema
    |> CommonSchema.create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "CRUD"
  @doc """
  Fetches a single struct from the data store where the primary key matches the given id.

  ## Options

  See [Ecto.Repo.get/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) for more information.
  """
  def get(queryable, id, opts \\ []) do
    Config.replica!(opts).get(queryable, id, opts)
  end

  @doc group: "CRUD"
  @doc """
  Fetches a single struct from the data store where the primary key matches the given params.

  ## Options

  See [Ecto.Repo.get_by/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get_by/3) for more information.
  """
  def find(queryable, params, opts \\ [])

  def find(query, params, opts)
      when params === %{} and not is_struct(query, Ecto.Query) do
    {:error,
     Error.call(
       :not_found,
       "record not found.",
       %{
         query: query,
         params: params
       },
       opts
     )}
  end

  def find(source, params, opts) do
    params =
      params
      |> put_param(opts, :order_by)
      |> put_param(opts, :group_by)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    case source
         |> CommonFilters.convert_params_to_filter(params, opts)
         |> Config.replica!(opts).one(opts) do
      nil ->
        {:error,
         Error.call(
           :not_found,
           "record not found.",
           %{
             query: source,
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
  Updates a record by id or by schema struct.
  """
  def update(queryable, id_or_schema_struct, params, opts \\ [])

  def update(queryable, id, params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, record} <- find(queryable, %{id: id}, opts) do
      update(queryable, record, params, opts)
    end
  end

  def update(queryable, schema_struct, params, opts) do
    queryable
    |> CommonSchema.create_changeset(schema_struct, params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc group: "CRUD"
  @doc """
  Deletes a record, changeset, or list of records/changesets.
  """
  def delete(data) do
    delete(data, [])
  end

  @doc group: "CRUD"
  @doc """
  Deletes a record, changeset, or list of records/changesets.
  """
  def delete(data, opts)

  def delete(queryable, id) when is_binary(id) or is_integer(id) do
    delete(queryable, id, [])
  end

  def delete(%{data: %{__meta__: %{schema: schema}}} = changeset, opts) do
    do_delete(changeset, schema, opts)
  end

  def delete(%{__meta__: %{schema: schema}} = schema_struct, opts) do
    do_delete(schema_struct, schema, opts)
  end

  def delete(records_or_changesets, opts) when is_list(records_or_changesets) do
    with {:ok, results} <-
           Enum.reduce_while(records_or_changesets, {:ok, []}, fn entry, {:ok, acc} ->
             case delete(entry, opts) do
               {:ok, result} -> {:cont, {:ok, [result | acc]}}
               {:error, reason} -> {:halt, {:error, reason}}
             end
           end) do
      {:ok, Enum.reverse(results)}
    end
  end

  @doc group: "CRUD"
  @doc """
  Deletes a record by id.
  """
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
  Streams records for the given query params.
  """
  def stream(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc group: "CRUD"
  @doc """
  Applies an aggregate operation to filtered records.
  """
  def aggregate(queryable, params \\ %{}, aggregate \\ :count, key \\ :id, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, key, opts)
  end

  @doc group: "CRUD"
  @doc """
  Finds a record and creates one when not found.
  """
  def find_and_create(queryable, find_params, create_params, opts \\ []) do
    with {:error, _} <- find(queryable, find_params, opts) do
      create(queryable, create_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds and updates a record.
  """
  def find_and_update(source, find_params, update_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      update(source, record, update_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds and upserts a record.
  """
  def find_and_upsert(source, find_params, upsert_params, opts \\ []) do
    case find(source, find_params, opts) do
      {:ok, record} -> update(source, record, upsert_params, opts)
      {:error, _} -> create(source, Map.merge(find_params, upsert_params), opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds and deletes a record.
  """
  def find_and_delete(source, find_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      delete(record, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record by query fields or creates it when not found.
  """
  def find_or_create(source, params, opts \\ []) do
    with {:error, _} <-
           find(
             source,
             Map.take(params, CommonSchema.get_query_fields(opts, source)),
             opts
           ) do
      source
      |> CommonSchema.get_schema_source()
      |> create(params, opts)
    end
  end

  @doc group: "Transaction"
  @doc """
  Runs the given function or multi in a transaction.
  """
  def transaction(fun_or_multi, opts \\ []) do
    Config.repo!(opts).transaction(fun_or_multi, opts)
  end

  @doc group: "Transaction"
  @doc """
  Runs a transaction with normalized return values.
  """
  def transact(fun_or_multi, opts \\ [])

  def transact(%Ecto.Multi{} = multi, opts) do
    multi
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  def transact(fun, opts) when is_function(fun) do
    fn repo -> eval_transaction_fun(fun, repo, opts) end
    |> transaction(opts)
    |> normalize_transaction_response(opts)
  end

  @doc group: "Batch"
  @doc """
  Batches records by key(s) and cardinality.
  """
  def batch(schema, params, batch_keys \\ :id, cardinality \\ :many, opts \\ [])

  def batch(_schema, [], _batch_keys, _cardinality, _opts) do
    %{}
  end

  def batch(schema, params, batch_keys, cardinality, opts)
      when is_list(batch_keys) and cardinality in @cardinalities do
    batch_keys = Enum.uniq(batch_keys)

    case Batch.build_batch_params(schema, params, batch_keys, opts) do
      [] ->
        %{}

      batch_params ->
        schema
        |> CommonFilters.convert_params_to_filter(batch_params, opts)
        |> Config.repo!(opts).all(opts)
        |> Enum.group_by(&Map.take(&1, batch_keys))
        |> Batch.handle_batch_response(cardinality, batch_keys)
    end
  end

  def batch(schema, params, batch_key, cardinality, opts)
      when cardinality in @cardinalities do
    values =
      params
      |> Enum.map(&Batch.normalize_batch_key(&1, batch_key))
      |> Enum.uniq()

    if values === [] do
      %{}
    else
      schema
      |> CommonFilters.convert_params_to_filter(%{batch_key => values}, opts)
      |> Config.repo!(opts).all(opts)
      |> Enum.group_by(&Batch.normalize_batch_key(&1, batch_key))
      |> Batch.handle_batch_response(cardinality, batch_key)
    end
  end

  @doc group: "Batch"
  @doc """
  Preloads batch lookup records and zips them with original entries.
  """
  def batch_preload(schema, entries, keys, opts \\ []) do
    {params_list, index_to_key} = Batch.extract_lookup_params(entries, keys)

    key_fields = Batch.normalize_key_fields(keys)

    fetched_records = batch(schema, params_list, key_fields, :one, opts)

    Enum.reduce(index_to_key, entries, fn {index, batch_key}, acc ->
      case Map.get(fetched_records, batch_key) do
        nil ->
          acc

        record ->
          current_entry = get_in(acc, [Access.at!(index)])
          updated_entry = Batch.zip_batch_result(current_entry, record)
          put_in(acc, [Access.at!(index)], updated_entry)
      end
    end)
  end

  @doc group: "Bulk"
  @doc """
  Inserts many records from a params list.

  [%{title: "A"}, {%Data{}, %{title: "B"}}, {%{id: 1}, %{title: "C"}}]
  """
  def insert_all(source, params_list, opts \\ []) do
    params_list =
      if Keyword.has_key?(opts, :preload) do
        batch_preload(source, params_list, opts[:preload], opts)
      else
        params_list
      end

    with {:ok, inserts} <- CommonParams.convert_to_insert_params(source, params_list, opts) do
      on_conflict_options = CommonParams.build_on_conflict_options(source, inserts, opts)

      {:ok,
       Config.repo!(opts).insert_all(
         source,
         inserts,
         Keyword.merge(on_conflict_options, opts)
       )}
    end
  end

  @doc group: "Bulk"
  @doc """
  Updates all records that match the find params.
  """
  def update_all(source, find_params, update_params, opts \\ []) do
    updates =
      source
      |> CommonSchema.get_schema_source()
      |> CommonParams.convert_to_update_params(update_params, opts)

    source
    |> CommonFilters.convert_params_to_filter(find_params, opts)
    |> Config.repo!(opts).update_all(updates, opts)
  end

  @doc group: "Bulk"
  @doc """
  Deletes all records that match the params.
  """
  def delete_all(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end

  @doc group: "Multi"
  @doc """
  Creates many records in a transaction.
  """
  def create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_create_many_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Finds many records in a transaction.
  """
  def find_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_find_many_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Updates many records in a transaction.
  """
  def update_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> Multi.build_update_many_multi(entries, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Deletes many records in a transaction.
  """
  def delete_many(schema, records, opts \\ []) when is_list(records) do
    schema
    |> Multi.build_delete_many_multi(records, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Finds or creates many records in a transaction.
  """
  def find_or_create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_find_or_create_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Finds and upserts many records in a transaction.
  """
  def find_and_upsert_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> Multi.build_upsert_multi(entries, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  defp do_delete(schema_data, schema, opts) do
    with {:error, failed_changeset} <-
           schema
           |> CommonSchema.create_changeset(schema_data, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "failed to delete record.",
         %{
           schema: schema,
           changeset: failed_changeset
         },
         opts
       )}
    end
  end

  defp put_param(enum, opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        enum

      value ->
        if is_map(enum) do
          Map.put(enum, key, value)
        else
          enum ++ [{key, value}]
        end
    end
  end

  defp normalize_transaction_response(result, opts) do
    unwrap? = Keyword.get(opts, :strict, true)

    case {unwrap?, result} do
      {_, {:error, :error}} ->
        :error

      {_, {:ok, :ok}} ->
        :ok

      {true, {:ok, {:error, _} = error}} ->
        error

      {true, {:ok, {:ok, _} = response}} ->
        response

      {_, other} ->
        other
    end
  end

  defp eval_transaction_fun(fun, repo, opts) do
    response = call_transaction_fun(fun, repo)

    if Keyword.get(opts, :strict, true) do
      maybe_rollback(response, repo)
    else
      response
    end
  end

  defp call_transaction_fun(fun, repo) when is_function(fun, 1), do: fun.(repo)
  defp call_transaction_fun(fun, _repo) when is_function(fun, 0), do: fun.()

  defp maybe_rollback(:error, repo), do: repo.rollback(:error)
  defp maybe_rollback({:error, reason}, repo), do: repo.rollback(reason)
  defp maybe_rollback({:ok, value}, _repo), do: value
  defp maybe_rollback(term, _repo), do: term
end
