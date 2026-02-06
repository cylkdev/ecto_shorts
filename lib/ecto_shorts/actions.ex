defmodule EctoShorts.Actions do
  @moduledoc """
  Public actions API for common CRUD, batch, bulk, multi, and transaction operations.
  """

  alias EctoShorts.Actions.Error

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
      |> Keyword.drop([:repo, :replica, :expression_adapter])
      |> Map.new()

    all(queryable, params, Keyword.take(opts, [:repo, :replica, :expression_adapter]))
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
      |> put_order_by(opts)
      |> put_group_by(opts)

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
      |> put_order_by(opts)
      |> put_group_by(opts)

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
    with {:error, failed_changeset} <-
           schema
           |> CommonSchema.create_changeset(changeset, opts)
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

  def delete(%{__meta__: %{schema: schema}} = schema_struct, opts) do
    with {:error, failed_changeset} <-
           schema
           |> CommonSchema.create_changeset(schema_struct, opts)
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
             Map.take(params, get_query_fields(opts, source)),
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
    |> handle_multi_response(opts)
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

  @doc group: "Batch"
  @doc """
  Preloads batch lookup records and zips them with original entries.
  """
  def batch_preload(schema, entries, keys, opts \\ []) do
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
    |> build_create_many_multi(params_list, opts)
    |> transaction(opts)
    |> handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Finds many records in a transaction.
  """
  def find_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> build_find_many_multi(params_list, opts)
    |> transaction(opts)
    |> handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Updates many records in a transaction.
  """
  def update_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> build_update_many_multi(entries, opts)
    |> transaction(opts)
    |> handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Deletes many records in a transaction.
  """
  def delete_many(schema, records, opts \\ []) when is_list(records) do
    schema
    |> build_delete_many_multi(records, opts)
    |> transaction(opts)
    |> handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Finds or creates many records in a transaction.
  """
  def find_or_create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> build_find_or_create_multi(params_list, opts)
    |> transaction(opts)
    |> handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc """
  Finds and upserts many records in a transaction.
  """
  def find_and_upsert_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> build_upsert_multi(entries, opts)
    |> transaction(opts)
    |> handle_multi_response(opts)
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

        {_, grouped_values} ->
          {key, grouped_values}
      end
    end)
    |> Map.new()
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

  defp build_create_many_multi(schema, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, index}, multi ->
      Ecto.Multi.run(multi, {:create, index}, fn repo, _changes ->
        repo_create(repo, schema, params, index, opts)
      end)
    end)
  end

  defp build_find_many_multi(schema, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, index}, multi ->
      Ecto.Multi.run(multi, {:find, index}, fn repo, _changes ->
        repo_find(repo, schema, params, index, opts)
      end)
    end)
  end

  defp build_update_many_multi(schema, entries, opts) do
    entries
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {arg, index}, multi ->
      Ecto.Multi.run(multi, {:update, index}, fn repo, _changes ->
        run_multi_update_many(repo, schema, arg, index, opts)
      end)
    end)
  end

  defp run_multi_update_many(repo, schema, {find_params, update_params}, index, opts) do
    with {:ok, record} <- repo_find(repo, schema, find_params, index, opts) do
      repo_update(repo, schema, record, update_params, index, opts)
    end
  end

  defp run_multi_update_many(repo, schema, %{id: id} = params, index, opts) do
    update_params = Map.delete(params, :id)

    with {:ok, record} <- repo_find(repo, schema, %{id: id}, index, opts) do
      repo_update(repo, schema, record, update_params, index, opts)
    end
  end

  defp run_multi_update_many(_repo, _schema, term, _index, _opts) do
    raise ArgumentError,
          """
          expected one of:

          - a tuple of {map(), map()}
          - a map with an :id key

          got: #{inspect(term)}
          """
  end

  defp build_delete_many_multi(schema, entries, opts) do
    entries
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {entry, index}, multi ->
      Ecto.Multi.run(multi, {:delete, index}, fn repo, _changes ->
        run_multi_delete(repo, schema, entry, index, opts)
      end)
    end)
  end

  defp run_multi_delete(repo, schema, %_{} = schema_struct, index, opts) do
    repo_delete(repo, schema, schema_struct, index, opts)
  end

  defp run_multi_delete(repo, schema, params, index, opts)
       when is_map(params) or is_list(params) do
    with {:ok, record} <- repo_find(repo, schema, params, index, opts) do
      repo_delete(repo, schema, record, index, opts)
    end
  end

  defp run_multi_delete(repo, schema, id, index, opts) do
    run_multi_delete(repo, schema, %{id: id}, index, opts)
  end

  defp build_find_or_create_multi(schema, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, index}, multi ->
      Ecto.Multi.run(multi, {:find_or_create, index}, fn repo, _changes ->
        case repo_one(repo, schema, params, opts) do
          nil -> repo_create(repo, schema, params, index, opts)
          record -> {:ok, record}
        end
      end)
    end)
  end

  defp build_upsert_multi(schema, entries, opts) do
    entries
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {arg, index}, multi ->
      case arg do
        {find_params, upsert_params} ->
          Ecto.Multi.run(multi, {:find_and_upsert, index}, fn repo, _changes ->
            repo_upsert(repo, schema, find_params, upsert_params, index, opts)
          end)

        %{id: id} = params ->
          upsert_params = Map.delete(params, :id)

          Ecto.Multi.run(multi, {:find_and_upsert, index}, fn repo, _changes ->
            repo_upsert(repo, schema, %{id: id}, upsert_params, index, opts)
          end)

        term ->
          raise ArgumentError,
                """
                expected one of:

                - a tuple of {map(), map()}
                - a map with an :id key

                got: #{inspect(term)}
                """
      end
    end)
  end

  defp repo_one(repo, schema, find_params, opts) do
    schema
    |> CommonFilters.convert_params_to_filter(find_params, opts)
    |> repo.one(opts)
  end

  defp repo_find(repo, schema, params, index, opts) do
    case repo_one(repo, schema, params, opts) do
      nil ->
        {:error,
         {:not_found, "record not found.",
          %{
            schema: schema,
            action: :find,
            index: index,
            params: params
          }}}

      record ->
        {:ok, record}
    end
  end

  defp repo_create(repo, schema, params, index, opts) do
    case schema
         |> CommonSchema.create_changeset(params, opts)
         |> repo.insert(opts) do
      {:ok, record} ->
        {:ok, record}

      {:error, changeset} ->
        {:error,
         {:conflict, "failed to create record.",
          %{
            schema: schema,
            action: :create,
            index: index,
            params: params,
            changeset: changeset
          }}}
    end
  end

  defp repo_update(repo, schema, record, params, index, opts) do
    case schema
         |> CommonSchema.create_changeset(record, params, opts)
         |> repo.update(opts) do
      {:ok, updated_record} ->
        {:ok, updated_record}

      {:error, changeset} ->
        {:error,
         {:conflict, "failed to update record.",
          %{
            schema: schema,
            action: :update,
            index: index,
            params: params,
            changeset: changeset
          }}}
    end
  end

  defp repo_delete(repo, schema, record, index, opts) do
    case record |> schema.changeset(%{}) |> repo.delete(opts) do
      {:ok, deleted_record} ->
        {:ok, deleted_record}

      {:error, changeset} ->
        {:error,
         {:conflict, "failed to delete record.",
          %{
            schema: schema,
            action: :delete,
            index: index,
            changeset: changeset
          }}}
    end
  end

  defp repo_upsert(repo, schema, find_params, upsert_params, index, opts) do
    params = Map.merge(find_params, upsert_params)

    case repo_one(repo, schema, find_params, opts) do
      nil -> repo_create(repo, schema, params, index, opts)
      record -> repo_update(repo, schema, record, params, index, opts)
    end
  end

  defp handle_multi_response(
         {:error, _failed_operation, {code, message, details}, changes_so_far},
         opts
       ) do
    details =
      details
      |> Map.new()
      |> Map.put(:changes_so_far, Map.values(changes_so_far))

    {:error, Error.call(code, message, details, opts)}
  end

  defp handle_multi_response({:error, _failed_operation, reason, _changes_so_far}, _opts) do
    {:error, reason}
  end

  defp handle_multi_response({:ok, operations}, _opts) do
    {:ok, Map.values(operations)}
  end

  defp get_query_fields(opts, source) do
    Keyword.get(opts, :query_fields, CommonSchema.get_schema_reflection(source, :query_fields))
  end
end
