defmodule EctoShorts.Actions.Multi do
  alias EctoShorts.{
    Actions.Error,
    Actions.Transaction,
    CommonFilters,
    CommonSchema
  }

  @doc group: "Multi"
  @doc """
  ...
  """
  def create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> build_create_many_multi(params_list, opts)
    |> Transaction.transaction(opts)
    |> handle_multi_response(opts)
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

  @doc group: "Multi"
  @doc """
  ...
  """
  def find_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> build_find_many_multi(params_list, opts)
    |> Transaction.transaction(opts)
    |> handle_multi_response(opts)
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

  @doc group: "Multi"
  @doc """
  ...
  """
  def update_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> build_update_many_multi(entries, opts)
    |> Transaction.transaction(opts)
    |> handle_multi_response(opts)
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

  @doc group: "Multi"
  @doc """
  ...
  """
  def delete_many(schema, records, opts \\ []) when is_list(records) do
    schema
    |> build_delete_many_multi(records, opts)
    |> Transaction.transaction(opts)
    |> handle_multi_response(opts)
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

  defp run_multi_delete(repo, schema, %_{} = schema_data, index, opts) do
    repo_delete(repo, schema, schema_data, index, opts)
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

  @doc group: "Multi"
  @doc """
  ...
  """
  def find_or_create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> build_find_or_create_multi(params_list, opts)
    |> Transaction.transaction(opts)
    |> handle_multi_response(opts)
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

  @doc group: "Multi"
  @doc """
  ...
  """
  def find_and_upsert_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> build_upsert_multi(entries, opts)
    |> Transaction.transaction(opts)
    |> handle_multi_response(opts)
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
      {:ok, record} ->
        {:ok, record}

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
end
