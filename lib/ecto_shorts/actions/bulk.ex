defmodule EctoShorts.Actions.Bulk do
  alias EctoShorts.{
    Actions.Batch,
    Config,
    CommonFilters,
    CommonParams,
    CommonSchema
  }

  @doc group: "CRUD"
  @doc """
  ...
  """
  def insert_all(source, params_list, opts \\ []) do
    params_list =
      if Keyword.has_key?(opts, :preload) do
        Batch.batch_preload(source, params_list, opts[:preload], opts)
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

  @doc group: "CRUD"
  @doc """
  ...
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

  @doc group: "CRUD"
  @doc """
  ...
  """
  def delete_all(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end
end
