defmodule EctoShorts.Actions do
  @moduledoc """
  # EctoShorts.Actions

  ## Shared Options

  The following options are shared across all functions in this module:

    * `:replica` - Sets the ecto repo module for read operations.
      This option takes precedence over the `:repo` option if set.

    * `:repo` - Sets the ecto repo module for read and write operations.
  """
  alias EctoShorts.{
    Actions.Error,
    CommonFilters,
    CommonSchemas,
    Config
  }

  @id :id

  @doc group: "Transaction API"
  @doc since: "2.5.0"
  @doc """
  Runs the given function or `Ecto.Multi` inside a transaction.

  By default, the transaction is rolled back if the function returns
  `:error` or `{:error, reason}` and no exception is raised. You can
  control this behavior with the `:rollback_on_error` option.

  Raises an error if the option `:rollback_on_error` is true and the
  function does not return `:ok`, `:error`, `{:ok, value}`, or
  `{:error, reason}`).

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  Additional options include:

    * `:rollback_on_error` – When `true`, the transaction will be rolled
      back if the function returns `:error` or `{:error, reason}`. When
      `false`, the transaction will still commit even if the function
      returns an error tuple. Defaults to `true`.

  All options accepted by [`Ecto.Repo.transaction/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:transaction/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.transaction(MyApp.Schema, fn -> :ok end)
      iex> EctoShorts.Actions.transaction(MyApp.Schema, fn -> :ok end, repo: MyApp.Repo)
      iex> EctoShorts.Actions.transaction(MyApp.Schema, fn -> :error end, rollback_on_error: false)

      iex> EctoShorts.Actions.transaction({"users", MyApp.Schema}, fn -> :ok end)
      iex> EctoShorts.Actions.transaction({"users", MyApp.Schema}, fn -> :ok end, repo: MyApp.Repo)
      iex> EctoShorts.Actions.transaction({"users", MyApp.Schema}, fn -> :error end, rollback_on_error: false)
  """
  @spec transaction(fun_or_multi :: function() | Ecto.Multi.t()) ::
          {:ok, term()} | {:error, term()} | Ecto.Multi.failure()
  @spec transaction(
          fun_or_multi :: function() | Ecto.Multi.t(),
          opts :: keyword()
        ) :: {:ok, term()} | {:error, term()} | Ecto.Multi.failure()
  def transaction(fun_or_multi, opts \\ [])

  def transaction(%_{} = multi, opts) do
    Config.repo!(opts).transaction(multi, opts)
  end

  def transaction(fun, opts) do
    op = fn repo ->
      result = if is_function(fun, 1), do: fun.(repo), else: fun.()

      if Keyword.get(opts, :rollback_on_error, true) do
        case result do
          :error ->
            repo.rollback(:error)

          {:error, reason} ->
            repo.rollback(reason)

          {:ok, _} = res ->
            res

          :ok ->
            :ok

          term ->
            raise """
            The function executed within the repo transaction returned an unexpected result.

            Expected one of:

              - :ok
              - :error
              - {:ok, value}
              - {:error, reason}

            Got:

            #{inspect(term)}

            If the `:rollback_on_error` option is set to `false`, the function does not need
            to return one of the expected values.
            """
        end
      else
        result
      end
    end

    Config.repo!(opts).transaction(op, opts)
  end

  @doc group: "Query API"
  @doc """
  Retrieves a single record by it's primary key.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.get/3`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) are also supported.

  ## Examples

      iex> EctoShorts.Actions.get(MyApp.Schema, 1)
      iex> EctoShorts.Actions.get(MyApp.Schema, 1, repo: MyApp.Repo)

      iex> EctoShorts.Actions.get({"users", MyApp.Schema}, 1)
      iex> EctoShorts.Actions.get({"users", MyApp.Schema}, 1, repo: MyApp.Repo)
  """
  @spec get(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id :: integer() | binary()
        ) :: Ecto.Schema.t() | nil
  @spec get(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id :: integer() | binary(),
          options :: keyword()
        ) :: Ecto.Schema.t() | nil
  def get(query, id, opts \\ []) do
    Config.replica!(opts).get(query, id, opts)
  end

  @doc group: "Query API"
  @doc """
  A shorthand for `&all/3`, allowing for filters and options to be passed more conveniently.

  This function accepts either a map or a keyword list as the second argument:

    * **Map** – Used to specify filter parameters. These are applied to the query. See
      `EctoShorts.CommonFilters` for supported filters.

    * **Keyword list** – Used to pass options such as `:repo` or `:replica`.

  ## Options

  See `&all/3` for options.

  ## Examples

      iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1})
      iex> EctoShorts.Actions.all(MyApp.Schema, id: 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all(MyApp.Schema, id: 1, replica: MyApp.Repo)

      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, %{id: 1})
      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, id: 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, id: 1, replica: MyApp.Repo)
  """
  @spec all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: list(Ecto.Schema.t())
  @spec all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          opts :: keyword()
        ) :: list(Ecto.Schema.t())
  def all(query, params) when is_map(params) do
    all(query, params, [])
  end

  def all(query, opts) do
    params =
      opts
      |> Keyword.drop([:repo, :replica])
      |> Map.new()

    all(query, params, Keyword.take(opts, [:repo, :replica]))
  end

  @doc group: "Query API"
  @doc """
  Fetches all records matching the given query and filters.

  ### Filter Parameters

  To apply filters, pass a map as the second argument. For supported filters, see `EctoShorts.CommonFilters`.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  Additional options include:

    * `:group_by` – Group results by one or more fields.
    * `:order_by` – Sort results based on one or more fields.

  All options accepted by [`Ecto.Repo.all/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1})
      iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1}, prefix: "public")
      iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1}, replica: MyApp.Repo)

      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, %{id: 1})
      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, %{id: 1}, prefix: "public")
      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all({"users", MyApp.Schema}, replica: MyApp.Repo)
  """
  @spec all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: list(Ecto.Schema.t())
  def all(query, params, opts) do
    params =
      params
      |> put_order_by(opts)
      |> put_group_by(opts)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    query
    |> CommonFilters.convert_params_to_filter(params)
    |> Config.replica!(opts).all(opts)
  end

  @doc group: "Schema API"
  @doc """
  Inserts a new record.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.insert/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.create(MyApp.Schema, %{name: "Fira"})
      iex> EctoShorts.Actions.create(MyApp.Schema, %{name: "Fira"}, repo: MyApp.Repo)

      iex> EctoShorts.Actions.create({"users", MyApp.Schema}, %{name: "Fira"})
      iex> EctoShorts.Actions.create({"users", MyApp.Schema}, %{name: "Fira"}, repo: MyApp.Repo)
  """
  @spec create(query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  @spec create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  @spec create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  def create(query, params \\ %{}, opts \\ []) do
    query
    |> CommonSchemas.build_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "Query API"
  @doc since: "2.5.0"
  @doc """
  See `find_all/5` for more information.

  ## Options

  See `find_all/5` for more options.

  ## Examples

      iex> EctoShorts.Actions.find_all(MyApp.Schema, [1])
      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, [1])
  """
  @spec find_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          values :: list(integer() | binary())
        ) :: {:ok, list(Ecto.Schema.t())} | {:error, list(term())}
  def find_all(query, values) do
    find_all(query, values, [])
  end

  @doc group: "Query API"
  @doc since: "2.5.0"
  @doc """
  See `find_all/5` for more information.

  ## Options

    * `:primary_key` – The primary key to use for the query. Defaults to `:id`.

  See `find_all/5` for more options.

  ## Examples

      iex> EctoShorts.Actions.find_all(MyApp.Schema, [1], repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_all(MyApp.Schema, [1], replica: MyApp.Repo)

      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, [1], repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, [1], replica: MyApp.Repo)
  """
  @spec find_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          values :: list(integer() | binary()),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | {:error, list(term())}
  def find_all(query, values, opts) do
    find_all(query, primary_key(query, opts), values, opts)
  end

  @doc group: "Query API"
  @doc since: "2.5.0"
  @doc """
  See `find_all/5` for more information.

  ## Options

  See `find_all/5` for options.

  ## Examples

      iex> EctoShorts.Actions.find_all(MyApp.Schema, :id, [1], repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_all(MyApp.Schema, :id, [1], replica: MyApp.Repo)

      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, :id, [1], repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, :id, [1], replica: MyApp.Repo)
  """
  @spec find_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          key :: atom(),
          values :: list(integer() | binary()),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | {:error, list(term())}
  def find_all(query, key, values, opts) do
    find_all(query, key, values, %{}, opts)
  end

  @doc group: "Query API"
  @doc since: "2.5.0"
  @doc """
  Finds all records where a field matches one of the given values.

  ## Filter parameters

  Additional filters can be added to the query by passing `params`.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.all/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.find_all(MyApp.Schema, :id, [1], %{inserted_at: %{gte: ~U[2023-01-01 00:00:00Z]}}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_all(MyApp.Schema, :id, [1], %{inserted_at: %{gte: ~U[2023-01-01 00:00:00Z]}}, replica: MyApp.Repo)

      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, :id, [1], %{inserted_at: %{gte: ~U[2023-01-01 00:00:00Z]}}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_all({"users", MyApp.Schema}, :id, [1], %{inserted_at: %{gte: ~U[2023-01-01 00:00:00Z]}}, replica: MyApp.Repo)
  """
  @spec find_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          key :: atom(),
          values :: list(integer() | binary()),
          params :: map(),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | {:error, list(term())}
  def find_all(query, _key, [], params, _opts) do
    {:error,
     ErrorMessage.not_found("no records found", %{
       query: query,
       params: params
     })}
  end

  def find_all(query, key, values, params, opts) do
    params = Map.merge(params, %{key => values})

    case all(query, params, opts) do
      [] ->
        {:error,
         ErrorMessage.not_found("no records found", %{
           query: query,
           key: key,
           values: values,
           params: params
         })}

      schema_data_list ->
        batch_results = Map.new(schema_data_list, &{Map.fetch!(&1, key), &1})

        EctoShorts.Utils.reduce_all(values, fn batch_value ->
          case Map.get(batch_results, batch_value) do
            nil ->
              {:error,
               ErrorMessage.not_found("no record found", %{
                 query: query,
                 key: key,
                 value: batch_value,
                 params: params
               })}

            schema_data ->
              {:ok, schema_data}
          end
        end)
    end
  end

  @doc """
  Finds a record matching the given parameters, or creates one if none is found.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  Additional options include:

    * `:drop_associations` – If `true` deletes all associations from the
      parameters before searching for a record. Defaults to `true`.

  For more information, see `&find/3` and `&create/3`.

  ## Examples

      iex> EctoShorts.Actions.find_or_create(MyApp.Schema, %{name: "Fira"})
      iex> EctoShorts.Actions.find_or_create(MyApp.Schema, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_or_create(MyApp.Schema, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_or_create({"users", MyApp.Schema}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_or_create({"users", MyApp.Schema}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_or_create({"users", MyApp.Schema}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_or_create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  @spec find_or_create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  def find_or_create(query, params, opts \\ []) do
    queryable = CommonSchemas.get_schema_queryable(query)

    find_params =
      if Keyword.get(opts, :drop_associations, true) do
        drop_associations(params, queryable)
      else
        params
      end

    with {:error, %{code: :not_found}} <- find(query, find_params, opts) do
      create(query, params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Finds a schema by the given params and updates it with the
  merged result of `params` and `update_params`. If no record
  is found, a new one is created using the merged values.

  Accepts a map of params, a map of update params, and an
  optional keyword list of options.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  For more information, see `&find/3` and `&update/4`.

  ## Examples

      iex> EctoShorts.Actions.find_and_update(MyApp.Schema, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_update(MyApp.Schema, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_update(MyApp.Schema, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_and_update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_and_update(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  @spec find_and_update(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  def find_and_update(query, find_params, update_params, opts \\ []) do
    with {:ok, schema_data} <- find(query, find_params, opts) do
      update(query, schema_data, update_params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Finds a schema using the given params and either updates it or
  creates a new record with the merged result of `params` and
  `update_params`.

  If a matching record is found, it is updated. If not, a new
  record is created using the combined values.

  Accepts an optional keyword list of options.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  For more information, see `&find/3`, `&create/3 and `&update/4`.

  ## Examples

      iex> EctoShorts.Actions.find_and_upsert(MyApp.Schema, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_upsert(MyApp.Schema, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_upsert(MyApp.Schema, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_and_upsert({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_upsert({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_upsert({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_and_upsert(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  @spec find_and_upsert(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  def find_and_upsert(query, find_params, update_params, opts \\ []) do
    case find(query, find_params, opts) do
      {:ok, schema_data} ->
        update(query, schema_data, update_params, opts)

      {:error, %{code: :not_found}} ->
        create_params = Map.merge(find_params, update_params)

        create(query, create_params, opts)

      e ->
        e
    end
  end

  @doc group: "Query API"
  @doc """
  Fetches a single record.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  Additional options include:

    * `:group_by` – Group results by one or more fields.
    * `:order_by` – Sort results based on one or more fields.

  All options accepted by [`Ecto.Repo.one/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:one/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.find(MyApp.Schema, %{id: 1})
      iex> EctoShorts.Actions.find({"users", MyApp.Schema}, %{id: 1})

      iex> EctoShorts.Actions.find({"users", MyApp.Schema}, %{id: 1}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find({"users", MyApp.Schema}, %{id: 1}, replica: MyApp.Repo)
  """
  @spec find(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, term()}
  @spec find(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, term()}
  def find(query, params, opts \\ [])

  def find(query, params, _opts) when params === %{} do
    {:error,
     Error.call(:not_found, "record not found", %{
       query: query,
       params: params
     })}
  end

  def find(query, params, opts) do
    params =
      params
      |> put_order_by(opts)
      |> put_group_by(opts)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    result =
      query
      |> CommonFilters.convert_params_to_filter(params)
      |> Config.replica!(opts).one(opts)

    case result do
      nil ->
        {:error,
         Error.call(:not_found, "record not found", %{
           query: query,
           params: params
         })}

      struct ->
        {:ok, struct}
    end
  end

  @doc group: "Schema API"
  @doc """
  Updates a record by its primary key, or struct.

  This function retrieves a record by its primary key, or accepts
  an existing struct then applies the given update parameters,
  and saves the changes to the database.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.update/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:update/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.update(MyApp.Schema, 1, %{body: "example"})
      iex> EctoShorts.Actions.update(MyApp.Schema, %MyApp.Schema{id: 1, body: "old"}, %{body: "example"})

      iex> EctoShorts.Actions.update({"users", MyApp.Schema}, 1, %{body: "example"})
      iex> EctoShorts.Actions.update({"users", MyApp.Schema}, %MyApp.Schema{id: 1, body: "old"}, %{body: "example"})
  """
  @spec update(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id_or_struct :: integer() | binary() | Ecto.Schema.t(),
          update_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  def update(query, id_or_struct, update_params, opts \\ [])

  def update(query, id, update_params, opts) when is_integer(id) or is_binary(id) do
    case get(query, id, opts) do
      nil ->
        {:error,
         Error.call(:not_found, "record not found", %{
           query: query,
           params: %{id: id}
         })}

      schema_data ->
        update(query, schema_data, update_params, opts)
    end
  end

  def update(query, schema_data, update_params, opts) when is_list(update_params) do
    update(query, schema_data, Map.new(update_params), opts)
  end

  def update(query, schema_data, update_params, opts) do
    query
    |> CommonSchemas.build_changeset(schema_data, update_params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc """
  Deletes a record given existing data.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.delete/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.delete(%MyApp.Schema{})
      iex> EctoShorts.Actions.delete([%MyApp.Schema{}])
  """
  @spec delete(schema :: Ecto.Schema.t()) ::
          {:ok, list(Ecto.Schema.t())} | {:error, Ecto.Changeset.t() | term()}
  @spec delete(schemas :: list(Ecto.Schema.t())) ::
          {:ok, list(Ecto.Schema.t())} | {:error, list(Ecto.Changeset.t() | term())}
  def delete(%_{} = schema_data) do
    delete(schema_data, [])
  end

  def delete(schema_data_list) when is_list(schema_data_list) do
    EctoShorts.Utils.reduce_all(schema_data_list, fn schema_data ->
      delete(schema_data, [])
    end)
  end

  @doc """
  Deletes a struct by it's primary key, struct, list of structs,
  changeset, or list of changesets.

  ### Options

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.delete/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) for more options.

  ## Examples

      iex> EctoShorts.Actions.delete(%MyApp.Schema{})
      iex> EctoShorts.Actions.create(%MyApp.Schema{}, repo: MyApp.Repo)
  """
  @spec delete(
          changeset :: Ecto.Changeset.t(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
  def delete(%_{data: %_{__meta__: %{schema: schema}}} = changeset, opts) do
    with {:error, changeset} <- Config.repo!(opts).delete(changeset, opts) do
      {:error,
       Error.call(:unprocessable_entity, "Failed to delete the record.", %{
         query: schema,
         changeset: changeset,
         params: changeset.params
       })}
    end
  end

  #   def delete(%queryable{} = schema_data, opts) do
  #     changeset = build_changeset(queryable, schema_data, %{}, opts)

  #     case Config.repo!(opts).delete(changeset, opts) do
  #       {:error, changeset} ->
  #         {:error, Error.call(
  #           :internal_server_error,
  #           "Error deleting #{inspect(queryable)}",
  #           %{changeset: changeset, schema_data: schema_data}
  #         )}
  #       ok -> ok
  #     end
  #   end

  #   def delete(schema_data, opts) when is_list(schema_data) do
  #     schema_data |> Enum.map(&delete(&1, opts)) |> reduce_status_tuples()
  #   end

  #   def delete(query, id) when (is_binary(id) or is_integer(id)) do
  #     delete(query, id, default_opts())
  #   end

  #   @doc """
  #   Deletes a schema. Can also accept a keyword options list.

  #   ### Options

  #     * `:replica` - A module that uses `Ecto.Repo`. This option takes
  #       precedence over the `:repo` option and will be used to
  #       fetch the record if set.

  #     * `:repo` - A module that uses `Ecto.Repo`.

  #   See `find/3` and [Ecto.Repo.delete/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) for more options.

  #   ## Examples

  #       iex> EctoShorts.Actions.delete(MyApp.Schema, 1)
  #       iex> EctoShorts.Actions.delete(MyApp.Schema, "binary_id")
  #       iex> EctoShorts.Actions.delete(MyApp.Schema, "binary_id", repo: MyApp.Repo)
  #       iex> EctoShorts.Actions.delete({"source", MyApp.Schema}, 1)
  #       iex> EctoShorts.Actions.delete({"source", MyApp.Schema}, "binary_id")
  #       iex> EctoShorts.Actions.delete({"source", MyApp.Schema}, "binary_id", repo: MyApp.Repo)
  #   """
  #   @spec delete(
  #     query :: queryable() | source_queryable(),
  #     id :: id(),
  #     opts :: opts()
  #   ) :: {:ok, schema()} | {:error, any()}
  #   def delete(query, id, opts) when (is_integer(id) or is_binary(id)) do
  #     with {:ok, schema_data} <- find(query, %{id: id}, opts) do
  #       Config.repo!(opts).delete(schema_data, opts)
  #     end
  #   end

  defp drop_associations(params, queryable) do
    Map.drop(params, queryable.__schema__(:associations))
  end

  defp put_order_by(params, opts) do
    case Keyword.get(opts, :order_by) do
      nil -> params
      order_by -> Map.put(params, :order_by, order_by)
    end
  end

  defp put_group_by(params, opts) do
    case Keyword.get(opts, :group_by) do
      nil -> params
      group_by -> Map.put(params, :group_by, group_by)
    end
  end

  defp primary_key(query, opts) do
    queryable = CommonSchemas.get_schema_queryable(query)

    opts[:primary_key] ||
      :primary_key |> queryable.__schema__() |> List.first() ||
      @id
  end
end

# defmodule EctoShorts.Actions do
#   @moduledoc """
#   Actions for CRUD in ecto, these can be used by all schemas/queries

#   Generally we can define our contexts to be very reusable by creating
#   them to look something like this:

#   ```elixir
#   defmodule MyApp.Accounts do
#     alias EctoShorts.Actions
#     alias MyApp.Accounts.User

#     def all_users(params), do: Actions.all(User, params)
#     def find_user(params), do: Actions.find(User, params)
#   end
#   ```

#   We're then able to use this context with all filters that are
#   supported by `EctoShorts.CommonFilters` without having to create new queries

#   ```elixir
#   def do_something do
#     MyApp.Accounts.all_user(%{
#       first_name: %{ilike: "john"},
#       age: %{gte: 18},
#       priority_level: 5,
#       address: %{country: "Canada"}
#     })
#   end
#   ```

#   You can read more on reusable ecto code [here](https://learn-elixir.dev/blogs/creating-reusable-ecto-code)

#   ### Supporting multiple Repos

#   To support multiple repos, what we can do is pass arguments to the last parameter
#   of most `EctoShorts.Actions` calls

#   #### Example

#   ```elixir
#   defmodule MyApp.Accounts do
#     alias EctoShorts.Actions
#     alias MyApp.Accounts.User

#     def all_users(params), do: Actions.all(User, params, replica: MyApp.Repo.Replica)
#     def create_user(params), do: Actions.find(User, params, repo: MyApp.Repo)
#   end
#   ```
#   """
#   @type id :: binary() | integer()
#   @type source :: binary()
#   @type field :: atom()
#   @type params :: map()
#   @type params_list :: list(params)
#   @type query :: Ecto.Query.t()
#   @type queryable :: Ecto.Queryable.t()
#   @type source_queryable :: {source(), queryable()}
#   @type changeset :: Ecto.Changeset.t()
#   @type changesets :: list(changeset())
#   @type schema :: Ecto.Schema.t()
#   @type schemas :: list() | list(schema())
#   @type opts :: Keyword.t()
#   @type aggregate_options :: :avg | :count | :max | :min | :sum
#   @type schema_res :: {:ok, schema()} | {:error, any}

#   alias EctoShorts.{
#     Actions.Error,
#     CommonFilters,
#     CommonSchemas,
#     Config,
#     SchemaHelpers
#   }

#   @doc """
#   Fetches a single record where the primary key matches the given `id`.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.get/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.get(MyApp.Schema, 1)
#       iex> EctoShorts.Actions.get(MyApp.Schema, 1)
#       iex> EctoShorts.Actions.get({"source", MyApp.Schema}, 1)
#   """
#   @spec get(
#     query :: query() | queryable() | source_queryable(),
#     id :: id(),
#     options :: opts()
#   ) :: schema() | nil
#   @spec get(
#     query :: query() | queryable() | source_queryable(),
#     id :: id()
#   ) :: schema() | nil
#   def get(query, id, opts \\ []) do
#     Config.replica!(opts).get(query, id, opts)
#   end

#   @doc """
#   Fetches all records matching the given query.

#   See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.all(MyApp.Schema)
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema})
#       iex> EctoShorts.Actions.all(%Ecto.Query{})
#   """
#   @spec all(query :: queryable() | source_queryable()) :: schemas()
#   def all(query) do
#     all(query, default_opts())
#   end

#   @doc """
#   Fetches all records matching the given query.

#   ### Filter Parameters

#   When the parameters is a keyword list the options `:repo` and `:replica` can be set.

#   See `EctoShorts.CommonFilters` for more information.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1})
#       iex> EctoShorts.Actions.all(MyApp.Schema, id: 1, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.all(MyApp.Schema, id: 1, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema}, %{id: 1})
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema}, id: 1, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema}, id: 1, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.all(%Ecto.Query{}, %{id: 1})
#       iex> EctoShorts.Actions.all(%Ecto.Query{}, id: 1, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.all(%Ecto.Query{}, id: 1, replica: MyApp.Repo)
#   """
#   @spec all(
#     query :: query() | queryable() | source_queryable(),
#     params_or_opts :: params() | opts()
#   ) :: schemas()
#   def all(query, params) when is_map(params) do
#     all(query, params, default_opts())
#   end

#   def all(query, opts) do
#     query_params =
#       opts
#       |> Keyword.drop([:repo, :replica])
#       |> Map.new()

#     if Enum.any?(query_params) do
#       all(query, query_params, Keyword.take(opts, [:repo, :replica]))
#     else
#       all(query, %{}, Keyword.take(opts, [:repo, :replica]))
#     end
#   end

#   @doc """
#   Fetches all records matching the given query.

#   ### Filter Parameters

#   See `EctoShorts.CommonFilters` for more information.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#     * `:order_by` - Orders the fields based on one or more fields.

#   See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1}, prefix: "public")
#       iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.all(MyApp.Schema, %{id: 1}, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema}, %{id: 1}, prefix: "public")
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.all({"source", MyApp.Schema}, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.all(%Ecto.Query{}, %{id: 1}, prefix: "public")
#       iex> EctoShorts.Actions.all(%Ecto.Query{}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.all(%Ecto.Query{}, replica: MyApp.Repo)
#   """
#   @spec all(
#     query :: query() | queryable() | source_queryable(),
#     params :: params(),
#     opts :: opts()
#   ) :: schemas()
#   def all(query, params, opts)  do
#     order_by = Keyword.get(opts, :order_by, nil)

#     params = if order_by, do: Map.put(params || %{}, :order_by, order_by), else: params

#     query
#     |> CommonFilters.convert_params_to_filter(params)
#     |> Config.replica!(opts).all(opts)
#   end

#   @doc """
#   Finds a schema with matching params. Can also accept a keyword options list.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:one/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.find(MyApp.Schema, %{id: 1})
#       iex> EctoShorts.Actions.find({"source", MyApp.Schema}, %{id: 1})
#       iex> EctoShorts.Actions.find({"source", MyApp.Schema}, %{id: 1}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find({"source", MyApp.Schema}, %{id: 1}, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.find(%Ecto.Query{}, %{id: 1})
#       iex> EctoShorts.Actions.find(%Ecto.Query{}, %{id: 1}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find(%Ecto.Query{}, %{id: 1}, replica: MyApp.Repo)
#   """
#   @spec find(
#     query :: queryable() | source_queryable(),
#     params :: params(),
#     opts
#   ) :: schema_res | {:error, any}
#   @spec find(
#     query :: queryable() | source_queryable(),
#     params :: params()
#   ) :: schema_res | {:error, any}
#   def find(query, params, opts \\ [])

#   def find(query, params, _options) when params === %{} and is_atom(query) do
#     {:error, Error.call(:not_found, "record not found", %{
#       query: query,
#       params: params
#     })}
#   end

#   def find(query, params, opts) do
#     order_by = Keyword.get(opts, :order_by, nil)

#     params = if order_by, do: Map.put(params || %{}, :order_by, order_by), else: params

#     query
#     |> CommonFilters.convert_params_to_filter(params)
#     |> Config.replica!(opts).one(opts)
#     |> case do
#       nil ->
#         {:error, Error.call(:not_found, "record not found", %{
#           query: query,
#           params: params
#         })}

#       schema -> {:ok, schema}
#     end
#   end

#   @doc """
#   Creates a schema with given params. Can also accept a keyword options list.

#   ### Options

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.insert/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.create(MyApp.Schema, %{name: "Fira"})
#       iex> EctoShorts.Actions.create(MyApp.Schema, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.create({"source", MyApp.Schema}, %{name: "Fira"})
#       iex> EctoShorts.Actions.create({"source", MyApp.Schema}, %{name: "Fira"}, repo: MyApp.Repo)
#   """
#   @spec create(
#     query :: queryable() | source_queryable(),
#     params :: params(),
#     opts :: opts()
#   ) :: {:ok, schema()} | {:error, changeset()}
#   @spec create(
#     query :: queryable() | source_queryable(),
#     params :: params()
#   ) :: {:ok, schema()} | {:error, changeset()}
#   def create(query, params, opts \\ []) do
#     query
#     |> build_changeset(params, opts)
#     |> Config.repo!(opts).insert(opts)
#   end

#   @doc """
#   Finds a schema by params or creates one if it isn't found.
#   Can also accept a keyword options list.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See `find/3` and `create/3` for more information.

#   ## Examples

#       iex> EctoShorts.Actions.find_or_create(MyApp.Schema, %{name: "Fira"})
#       iex> EctoShorts.Actions.find_or_create(MyApp.Schema, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find_or_create(MyApp.Schema, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#       iex> EctoShorts.Actions.find_or_create({"source", MyApp.Schema}, %{name: "Fira"})
#       iex> EctoShorts.Actions.find_or_create({"source", MyApp.Schema}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find_or_create({"source", MyApp.Schema}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#   """
#   @spec find_or_create(
#     query :: queryable() | source_queryable(),
#     params :: params(),
#     opts :: opts()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
#   @spec find_or_create(
#     query :: queryable() | source_queryable(),
#     params :: params()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
#   def find_or_create(query, params, opts \\ []) do
#     queryable = CommonSchemas.get_schema_queryable(query)

#     find_params = drop_associations(params, queryable)

#     with {:error, %{code: :not_found}} <- find(query, find_params, opts) do
#       create(query, params, opts)
#     end
#   end

#   @doc """
#   Finds a schema by params and updates it or creates with results of
#   params/update_params merged. Can also accept a keyword options list.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See `find/3` and `update/4` for more information.

#   ## Examples

#       iex> EctoShorts.Actions.find_and_update(MyApp.Schema, %{id: 1}, %{name: "Fira"})
#       iex> EctoShorts.Actions.find_and_update(MyApp.Schema, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find_and_update(MyApp.Schema, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#       iex> EctoShorts.Actions.find_and_update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"})
#       iex> EctoShorts.Actions.find_and_update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find_and_update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#   """
#   @spec find_and_update(
#     query :: queryable() | source_queryable(),
#     find_params :: params(),
#     update_params :: params(),
#     opts :: opts()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
#   @spec find_and_update(
#     query :: queryable() | source_queryable(),
#     find_params :: params(),
#     update_params :: params()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
#   def find_and_update(query, find_params, update_params, opts \\ []) do
#     with {:ok, schema_data} <- find(query, find_params, opts) do
#       update(query, schema_data, update_params, opts)
#     end
#   end

#   @doc """
#   Finds a schema by params and updates it or creates with results of
#   params/update_params merged. Can also accept a keyword options list.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See `find/3`, `create/3` and `update/4` for more information.

#   ## Examples

#       iex> EctoShorts.Actions.find_and_upsert(MyApp.Schema, %{id: 1}, %{name: "Fira"})
#       iex> EctoShorts.Actions.find_and_upsert(MyApp.Schema, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find_and_upsert(MyApp.Schema, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#       iex> EctoShorts.Actions.find_and_upsert({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"})
#       iex> EctoShorts.Actions.find_and_upsert({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.find_and_upsert({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#   """
#   @spec find_and_upsert(
#     query :: queryable() | source_queryable(),
#     find_params :: params(),
#     update_params :: params(),
#     opts :: opts()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
#   @spec find_and_upsert(
#     query :: queryable() | source_queryable(),
#     find_params :: params(),
#     update_params :: params()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t() | term()}
#   def find_and_upsert(query, find_params, update_params, opts \\ []) do
#     case find(query, find_params, opts) do
#       {:ok, schema_data} ->
#         update(query, schema_data, update_params, opts)

#       {:error, %{code: :not_found}} ->
#         create_params = Map.merge(find_params, update_params)

#         create(query, create_params, opts)

#       e -> e
#     end
#   end

#   @doc """
#   Updates a schema with given updates. Can also accept a keyword options list.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See `update/4` and [Ecto.Repo.get/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.update(MyApp.Schema, %{id: 1}, %{name: "Fira"})
#       iex> EctoShorts.Actions.update(MyApp.Schema, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.update(MyApp.Schema, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#       iex> EctoShorts.Actions.update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"})
#       iex> EctoShorts.Actions.update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.update({"source", MyApp.Schema}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
#   """
#   @spec update(
#     query :: queryable() | source_queryable(),
#     id :: pos_integer | String.t(),
#     updates :: map() | Keyword.t()
#   ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
#   @spec update(
#     query :: queryable() | source_queryable(),
#     id :: pos_integer | String.t(),
#     updates :: map() | Keyword.t(),
#     opts
#   ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
#   @spec update(
#     query :: queryable() | source_queryable(),
#     schema_data :: Ecto.Schema.t(),
#     updates :: map() | Keyword.t()
#   ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
#   @spec update(
#     query :: queryable() | source_queryable(),
#     schema_data :: Ecto.Schema.t(),
#     updates :: map() | Keyword.t(),
#     opts
#   ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
#   def update(query, schema_data, update_params, opts \\ [])

#   def update(query, schema_id, update_params, opts) when is_integer(schema_id) or is_binary(schema_id) do
#     case get(query, schema_id, opts) do
#       nil ->
#         {:error, Error.call(
#           :not_found,
#           "No item found with id: #{schema_id}",
#           %{
#             schema: query,
#             schema_id: schema_id,
#             updates: update_params
#           }
#         )}
#       schema_data -> update(query, schema_data, update_params, opts)
#     end
#   end

#   def update(query, schema_data, update_params, opts) when is_list(update_params) do
#     update_params = Map.new(update_params)

#     update(query, schema_data, update_params, opts)
#   end

#   def update(query, schema_data, update_params, opts) do
#     query
#     |> build_changeset(schema_data, update_params, opts)
#     |> Config.repo!(opts).update(opts)
#   end

#   @doc """
#   Deletes a record given existing data.

#   ## Examples

#       iex> EctoShorts.Actions.delete(%MyApp.Schema{})
#       iex> EctoShorts.Actions.delete([%MyApp.Schema{}])
#   """
#   @spec delete(schema :: schema()) :: {:ok, schema()} | {:error, any()}
#   @spec delete(schemas :: schemas()) :: {:ok, schemas()} | {:error, any()}
#   def delete(%_{} = schema_data) do
#     delete(schema_data, default_opts())
#   end

#   def delete(schema_data) when is_list(schema_data) do
#     delete(schema_data, default_opts())
#   end

#   @doc """
#   Similar to `delete/1` but can also accept a keyword options list.

#   ### Options

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.delete/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.delete(%MyApp.Schema{})
#       iex> EctoShorts.Actions.create(%MyApp.Schema{}, repo: MyApp.Repo)
#   """
#   @spec delete(
#     schema :: schema(),
#     opts :: opts()
#   ) :: {:ok, schema()} | {:error, any()}
#   @spec delete(
#     schemas :: schemas(),
#     opts :: opts()
#   ) :: {:ok, schemas()} | {:error, any()}
#   @spec delete(
#     query :: queryable() | source_queryable(),
#     id :: id()
#   ) :: {:ok, schema()} | {:error, any()}
#   def delete(%Ecto.Changeset{} = changeset, opts) do
#     case Config.repo!(opts).delete(changeset, opts) do
#       {:error, changeset} ->
#         {:error, Error.call(
#           :internal_server_error,
#           "Error deleting #{inspect(changeset.data.__struct__)}",
#           %{changeset: changeset}
#         )}
#       ok -> ok
#     end
#   end

#   def delete(%queryable{} = schema_data, opts) do
#     changeset = build_changeset(queryable, schema_data, %{}, opts)

#     case Config.repo!(opts).delete(changeset, opts) do
#       {:error, changeset} ->
#         {:error, Error.call(
#           :internal_server_error,
#           "Error deleting #{inspect(queryable)}",
#           %{changeset: changeset, schema_data: schema_data}
#         )}
#       ok -> ok
#     end
#   end

#   def delete(schema_data, opts) when is_list(schema_data) do
#     schema_data |> Enum.map(&delete(&1, opts)) |> reduce_status_tuples()
#   end

#   def delete(query, id) when (is_binary(id) or is_integer(id)) do
#     delete(query, id, default_opts())
#   end

#   @doc """
#   Deletes a schema. Can also accept a keyword options list.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See `find/3` and [Ecto.Repo.delete/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.delete(MyApp.Schema, 1)
#       iex> EctoShorts.Actions.delete(MyApp.Schema, "binary_id")
#       iex> EctoShorts.Actions.delete(MyApp.Schema, "binary_id", repo: MyApp.Repo)
#       iex> EctoShorts.Actions.delete({"source", MyApp.Schema}, 1)
#       iex> EctoShorts.Actions.delete({"source", MyApp.Schema}, "binary_id")
#       iex> EctoShorts.Actions.delete({"source", MyApp.Schema}, "binary_id", repo: MyApp.Repo)
#   """
#   @spec delete(
#     query :: queryable() | source_queryable(),
#     id :: id(),
#     opts :: opts()
#   ) :: {:ok, schema()} | {:error, any()}
#   def delete(query, id, opts) when (is_integer(id) or is_binary(id)) do
#     with {:ok, schema_data} <- find(query, %{id: id}, opts) do
#       Config.repo!(opts).delete(schema_data, opts)
#     end
#   end

#   @doc """
#   Returns a lazy enumerable that emits all entries matching the given query.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.stream/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:stream/2) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.stream(MyApp.Schema, %{id: 1})
#       iex> EctoShorts.Actions.stream(MyApp.Schema, %{id: 1}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.stream(MyApp.Schema, %{id: 1}, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.stream({"source", MyApp.Schema}, %{id: 1})
#       iex> EctoShorts.Actions.stream({"source", MyApp.Schema}, %{id: 1}, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.stream({"source", MyApp.Schema}, %{id: 1}, replica: MyApp.Repo)
#   """
#   @spec stream(
#     query :: queryable() | source_queryable(),
#     params :: params(),
#     opts :: opts()
#   ) :: schemas()
#   @spec stream(
#     query :: queryable() | source_queryable(),
#     params :: params()
#   ) :: schemas()
#   def stream(query, params, opts \\ []) do
#     query
#     |> CommonSchemas.get_schema_query()
#     |> CommonFilters.convert_params_to_filter(params)
#     |> Config.replica!(opts).stream(opts)
#   end

#   @doc """
#   Calculate the given aggregate.

#   ### Options

#     * `:replica` - A module that uses `Ecto.Repo`. This option takes
#       precedence over the `:repo` option and will be used to
#       fetch the record if set.

#     * `:repo` - A module that uses `Ecto.Repo`.

#   See [Ecto.Repo.aggregate/4](https://hexdocs.pm/ecto/Ecto.Repo.html#c:aggregate/4) for more options.

#   ## Examples

#       iex> EctoShorts.Actions.aggregate(MyApp.Schema, %{id: 1}, :count, :id)
#       iex> EctoShorts.Actions.aggregate(MyApp.Schema, %{id: 1}, :count, :id, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.aggregate(MyApp.Schema, %{id: 1}, :count, :id, replica: MyApp.Repo)
#       iex> EctoShorts.Actions.aggregate({"source", MyApp.Schema}, %{id: 1}, :count, :id)
#       iex> EctoShorts.Actions.aggregate({"source", MyApp.Schema}, %{id: 1}, :count, :id, repo: MyApp.Repo)
#       iex> EctoShorts.Actions.aggregate({"source", MyApp.Schema}, %{id: 1}, :count, :id, replica: MyApp.Repo)
#   """
#   @spec aggregate(
#     query :: query() | queryable() | source_queryable(),
#     params :: params(),
#     aggregate :: aggregate_options(),
#     field :: field(),
#     opts :: opts()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, any()}
#   @spec aggregate(
#     query :: query() | queryable() | source_queryable(),
#     params :: params(),
#     aggregate :: aggregate_options(),
#     field :: field()
#   ) :: {:ok, Ecto.Schema.t()} | {:error, any()}
#   def aggregate(query, params, aggregate, field, opts \\ []) do
#     query
#     |> CommonSchemas.get_schema_query()
#     |> CommonFilters.convert_params_to_filter(params)
#     |> Config.replica!(opts).aggregate(aggregate, field, opts)
#   end

#   @doc """
#   Accepts a list of schemas and attempts to find them in the DB. Any missing Schemas will be created.
#   Can also accept a keyword options list.

#   ***Note: Relational filtering doesn't work on this function***

#   ## Options
#     * `:repo` - A module that uses the Ecto.Repo Module.
#     * `:replica` - If you don't want to perform any reads against your Primary, you can specify a replica to read from.

#   ## Examples
#     iex> {:ok, records} = EctoShorts.Actions.find_or_create_many(EctoShorts.Accounts.User, [%{name: "foo"}, %{name: "bar}])
#     iex> length(records) === 2
#   """
#   @spec find_or_create_many(
#     query :: queryable() | source_queryable(),
#     params_list :: params_list(),
#     opts :: opts()
#   ) :: {:ok, schemas()} | {:error, changesets()}
#   @spec find_or_create_many(
#     query :: queryable() | source_queryable(),
#     params_list :: params_list()
#   ) :: {:ok, schemas()} | {:error, changesets()}
#   def find_or_create_many(query, param_list, opts) do
#     queryable = CommonSchemas.get_schema_queryable(query)

#     find_param_list =
#       Enum.map(param_list, fn params ->
#         drop_associations(params, queryable)
#       end)

#     {create_params, found_results} = find_many(query, find_param_list, opts)

#     query
#     |> multi_insert(param_list, create_params, opts)
#     |> Config.repo!(opts).transaction()
#     |> case do
#       {:ok, created_map} -> {:ok, merge_found(created_map, found_results)}
#       error -> error
#     end
#   end

#   def find_or_create_many(schema, param_list) do
#     find_or_create_many(schema, param_list, default_opts())
#   end

#   defp find_many(schema, param_list, opts) do
#     param_list
#     |> Enum.map(fn params ->
#       case find(schema, params, opts) do
#         {:ok, schema_data} -> schema_data
#         _ -> nil
#       end
#     end)
#     |> Enum.with_index()
#     |> Enum.split_with(fn {schema_data, _index} -> is_nil(schema_data) end)
#   end

#   defp multi_insert(queryable, param_list, create_params, opts) do
#     Enum.reduce(create_params, Ecto.Multi.new(), fn {nil, i}, multi ->
#       Ecto.Multi.insert(multi, i, fn _ ->
#         build_changeset(queryable, Enum.at(param_list, i), opts)
#       end)
#     end)
#   end

#   defp build_changeset({source, queryable}, schema_data, params, opts) do
#     schema_data = SchemaHelpers.build_struct(schema_data, source: source)

#     build_changeset(queryable, schema_data, params, opts)
#   end

#   defp build_changeset(queryable, schema_data, params, opts) do
#     case opts[:changeset] do
#       nil ->
#         queryable.changeset(schema_data, params)

#       func when is_function(func, 2) ->
#         func.(schema_data, params)

#       func when is_function(func, 1) ->
#         schema_data
#         |> queryable.changeset(params)
#         |> func.()
#     end
#   end

#   defp build_changeset({source, queryable}, params, opts) do
#     loaded_struct = CommonSchemas.get_loaded_struct({source, queryable})

#     build_changeset(queryable, loaded_struct, params, opts)
#   end

#   defp build_changeset(queryable, params, opts) do
#     if Code.ensure_loaded?(queryable) and function_exported?(queryable, :create_changeset, 1) do
#       queryable.create_changeset(params)
#     else
#       struct = struct(queryable)

#       build_changeset(queryable, struct, params, opts)
#     end
#   end

#   defp drop_associations(params, queryable) do
#     Map.drop(params, queryable.__schema__(:associations))
#   end

#   defp merge_found(created_map, found_results) do
#     created_map
#     |> Enum.map(fn {index, result} -> {result, index} end)
#     |> Kernel.++(found_results)
#     |> Enum.sort(&(elem(&1, 1) >= elem(&2, 1)))
#     |> Enum.map(&elem(&1, 0))
#   end

#   defp reduce_status_tuples(status_tuples) do
#     {status, res} =
#       Enum.reduce(status_tuples, {:ok, []}, fn
#         {:ok, _}, {:error, _} = e -> e
#         {:ok, record}, {:ok, acc} -> {:ok, [record | acc]}
#         {:error, error}, {:ok, _} -> {:error, [error]}
#         {:error, e}, {:error, error_acc} -> {:error, [e | error_acc]}
#       end)

#     {status, Enum.reverse(res)}
#   end

#   defp default_opts do
#     [repo: Config.repo(), replica: Config.replica()]
#   end
# end
