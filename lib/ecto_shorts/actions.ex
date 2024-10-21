defmodule EctoShorts.Actions do
  @moduledoc """
  Actions for CRUD in ecto, these can be used by all schema_list/queries

  Generally we can define our contexts to be very reusable by creating
  them to look something like this:

  ```elixir
  defmodule MyApp.Accounts do
    alias EctoShorts.Actions
    alias MyApp.Accounts.User

    def all_users(params), do: Actions.all(User, params)
    def find_user(params), do: Actions.find(User, params)
  end
  ```

  We're then able to use this context with all filters that are
  supported by `EctoShorts.CommonFilters` without having to create new queries

  ```elixir
  def do_something do
    MyApp.Accounts.all_user(%{
      first_name: %{ilike: "john"},
      age: %{gte: 18},
      priority_level: 5,
      address: %{country: "Canada"}
    })
  end
  ```

  You can read more on reusable ecto code [here](https://learn-elixir.dev/blogs/creating-reusable-ecto-code)

  ### Supporting multiple Repos

  To support multiple repos, what we can do is pass arguments to the last parameter
  of most `EctoShorts.Actions` calls

  #### Example

  ```elixir
  defmodule MyApp.Accounts do
    alias EctoShorts.Actions
    alias MyApp.Accounts.User

    def all_users(params), do: Actions.all(User, params, replica: MyApp.Repo.Replica)
    def create_user(params), do: Actions.find(User, params, repo: MyApp.Repo)
  end
  ```

  ## Shared Options

    * `changeset` - Modifies the changeset given to the `Ecto.Repo` api. A changeset
      is first built using `changeset/2` function from the named struct module (the
      `Ecto.Schema` module) before this is applied. This means that any pre-existing
      modifications (eg. constraints) are applied first. The value can be any of the
      following:

        * `{module, function, args}` - The `function` in the `module` will be invoked
          with the changeset prepended to the `args`. For example given the args
          `[params]` it will be called as `module.fun.(changeset, params)`.

        * `2-arity function` - A 2-arity function invoked as `fun.(changeset, params)`.

        * `1-arity function` - A 1-arity function invoked as `fun.(changeset)`.
  """
  @type id :: binary() | integer()
  @type source :: binary()
  @type field :: atom()
  @type params :: map()
  @type params_list :: list(params)
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}
  @type changeset :: Ecto.Changeset.t()
  @type schema :: Ecto.Schema.t()
  @type opts :: Keyword.t()
  @type aggregate_options :: :avg | :count | :max | :min | :sum
  @type schema_res :: {:ok, schema()} | {:error, any}

  alias EctoShorts.{
    Actions.Error,
    CommonFilters,
    CommonSchemas,
    Config
  }

  @doc """
  Fetches a single record where the primary key matches the given `id`.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.get/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) for more options.

  ### Examples

      iex> EctoSchemas.Actions.get(YourSchema, 1)
      iex> EctoSchemas.Actions.get(YourSchema, 1)
      iex> EctoSchemas.Actions.get({"source", YourSchema}, 1)
  """
  @spec get(
    query :: query() | queryable() | source_queryable(),
    id :: id(),
    options :: opts()
  ) :: schema() | nil
  @spec get(
    query :: query() | queryable() | source_queryable(),
    id :: id()
  ) :: schema() | nil
  def get(query, id, opts \\ []) do
    Config.replica!(opts).get(query, id, opts)
  end

  @doc """
  Fetches all records matching the given query.

  See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.all(YourSchema)
      iex> EctoSchemas.Actions.all({"source", YourSchema})
      iex> EctoSchemas.Actions.all(%Ecto.Query{})
  """
  @spec all(query :: query() | queryable() | source_queryable()) :: list(schema())
  def all(query) do
    all(query, default_opts())
  end

  @doc """
  Fetches all records matching the given query.

  ### Filter Parameters

  When the parameters is a keyword list the options `:repo` and `:replica` can be set.

  See `EctoShorts.CommonFilters` for more information.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.all(YourSchema, %{id: 1})
      iex> EctoSchemas.Actions.all(YourSchema, id: 1, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.all(YourSchema, id: 1, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.all({"source", YourSchema}, %{id: 1})
      iex> EctoSchemas.Actions.all({"source", YourSchema}, id: 1, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.all({"source", YourSchema}, id: 1, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.all(%Ecto.Query{}, %{id: 1})
      iex> EctoSchemas.Actions.all(%Ecto.Query{}, id: 1, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.all(%Ecto.Query{}, id: 1, replica: YourApp.Repo)
  """
  @spec all(
    query :: query() | queryable() | source_queryable(),
    params :: params()
  ) :: list(schema())
  @spec all(
    query :: query() | queryable() | source_queryable(),
    opts :: opts()
  ) :: list(schema())
  def all(query, params) when is_map(params) do
    all(query, params, default_opts())
  end

  def all(query, opts) do
    query_params =
      opts
      |> Keyword.drop([:repo, :replica])
      |> Map.new()

    all(query, query_params, Keyword.take(opts, [:repo, :replica]))
  end

  @doc """
  Fetches all records matching the given query.

  ### Filter Parameters

  See `EctoShorts.CommonFilters` for more information.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used if set.

    * `:repo` - A module that uses `Ecto.Repo`.

    * `:group_by` - Groups together rows from the schema that have the same values in the given fields.

    * `:order_by` - Orders the fields based on one or more fields.

  See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for more options.

  ## Examples

      iex> EctoSchemas.Actions.all(YourSchema, %{id: 1}, prefix: "public")
      iex> EctoSchemas.Actions.all(YourSchema, %{id: 1}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.all(YourSchema, %{id: 1}, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.all({"source", YourSchema}, %{id: 1}, prefix: "public")
      iex> EctoSchemas.Actions.all({"source", YourSchema}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.all({"source", YourSchema}, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.all(%Ecto.Query{}, %{id: 1}, prefix: "public")
      iex> EctoSchemas.Actions.all(%Ecto.Query{}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.all(%Ecto.Query{}, replica: YourApp.Repo)
  """
  @spec all(
    query :: query() | queryable() | source_queryable(),
    params :: params(),
    opts :: opts()
  ) :: list(schema())
  def all(query, params, opts)  do
    params = put_order_by_and_group_by(params, opts)

    query
    |> CommonFilters.convert_params_to_filter(params)
    |> Config.replica!(opts).all(opts)
  end

  @doc """
  Creates a schema with given params. Can also accept a keyword options list.

  ### Options

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.insert/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.create(YourSchema, %{name: "example"})
      iex> EctoSchemas.Actions.create(YourSchema, %{name: "example"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.create({"source", YourSchema}, %{name: "example"})
      iex> EctoSchemas.Actions.create({"source", YourSchema}, %{name: "example"}, repo: YourApp.Repo)
  """
  @spec create(
    query :: queryable() | source_queryable(),
    params :: params(),
    opts :: opts()
  ) :: {:ok, schema()} | {:error, changeset()}
  @spec create(
    query :: queryable() | source_queryable(),
    params :: params()
  ) :: {:ok, schema()} | {:error, changeset()}
  @spec create(
    query :: queryable() | source_queryable()
  ) :: {:ok, schema()} | {:error, changeset()}
  def create(query, params \\ %{}, opts \\ []) do
    query
    |> prepare_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc """
  Finds a schema with matching params. Can also accept a keyword options list.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used if set.

    * `:repo` - A module that uses `Ecto.Repo`.

    * `:group_by` - Groups together rows from the schema that have the same values in the given fields.

    * `:order_by` - Orders the fields based on one or more fields.

  See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:one/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.find(YourSchema, %{id: 1})
      iex> EctoSchemas.Actions.find({"source", YourSchema}, %{id: 1})
      iex> EctoSchemas.Actions.find({"source", YourSchema}, %{id: 1}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find({"source", YourSchema}, %{id: 1}, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.find(%Ecto.Query{}, %{id: 1})
      iex> EctoSchemas.Actions.find(%Ecto.Query{}, %{id: 1}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find(%Ecto.Query{}, %{id: 1}, replica: YourApp.Repo)
  """
  @spec find(
    query :: query() | queryable() | source_queryable(),
    params :: params(),
    opts
  ) :: schema_res | {:error, any}
  @spec find(
    query :: query() | queryable() | source_queryable(),
    params :: params()
  ) :: schema_res | {:error, any}
  def find(query, params, opts \\ [])

  def find({_source, _queryable} = query, params, _options) when params === %{} do
    {:error, Error.call(:not_found, "no records found", %{
      query: query,
      params: params
    })}
  end

  def find(query, params, _options) when params === %{} and is_atom(query) do
    {:error, Error.call(:not_found, "no records found", %{
      query: query,
      params: params
    })}
  end

  def find(query, params, opts) do
    params = put_order_by_and_group_by(params, opts)

    query
    |> CommonFilters.convert_params_to_filter(params)
    |> Config.replica!(opts).one(opts)
    |> case do
      nil ->
        {:error, Error.call(:not_found, "no records found", %{
          query: query,
          params: params
        })}

      schema -> {:ok, schema}
    end
  end

  @doc """
  Updates a schema with given updates. Can also accept a keyword options list.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See `update/4` and [Ecto.Repo.get/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) for more options.

  ### Examples

      iex> EctoSchemas.Actions.update(YourSchema, %{id: 1}, %{name: "great name"})
      iex> EctoSchemas.Actions.update(YourSchema, %{id: 1}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.update(YourSchema, %{id: 1}, %{name: "great name"}, replica: YourApp.Repo.replica())
      iex> EctoSchemas.Actions.update({"source", YourSchema}, %{id: 1}, %{name: "great name"})
      iex> EctoSchemas.Actions.update({"source", YourSchema}, %{id: 1}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.update({"source", YourSchema}, %{id: 1}, %{name: "great name"}, replica: YourApp.Repo.replica())
  """
  @spec update(
    query :: queryable() | source_queryable(),
    id :: pos_integer | String.t(),
    updates :: map() | Keyword.t()
  ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
  @spec update(
    query :: queryable() | source_queryable(),
    id :: pos_integer | String.t(),
    updates :: map() | Keyword.t(),
    opts
  ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
  @spec update(
    query :: queryable() | source_queryable(),
    schema_data :: Ecto.Schema.t(),
    updates :: map() | Keyword.t()
  ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
  @spec update(
    query :: queryable() | source_queryable(),
    schema_data :: Ecto.Schema.t(),
    updates :: map() | Keyword.t(),
    opts
  ) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t}
  def update(query, schema_data, update_params, opts \\ [])

  def update(query, id, update_params, opts) when is_integer(id) or is_binary(id) do
    case get(query, id, opts) do
      nil ->
        {:error, Error.call(:not_found, "No item found with id: #{id}", %{
          query: query,
          find_params: %{id: id},
          update_params: update_params
        })}

      schema_data -> update(query, schema_data, update_params, opts)
    end
  end

  def update(query, schema_data, update_params, opts) when is_list(update_params) do
    update(query, schema_data, Map.new(update_params), opts)
  end

  def update(query, schema_data, update_params, opts) do
    query
    |> prepare_changeset(schema_data, update_params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc """
  Deletes a record given existing data.

  ### Examples

      iex> EctoSchemas.Actions.delete(%YourSchema{})
      iex> EctoSchemas.Actions.delete([%YourSchema{}])
  """
  @spec delete(schema :: query() | changeset() | schema()) :: {:ok, schema()} | {:error, any()}
  @spec delete(schema_list :: list(schema())) :: {:ok, list(query() | changeset() | schema())} | {:error, any()}
  def delete(schema_data) do
    delete(schema_data, default_opts())
  end

  @doc """
  Similar to `delete/1` but can also accept a keyword options list.

  ### Options

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.delete/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.delete(%YourSchema{})
  """
  @spec delete(
    query_or_schema :: query() | queryable() | source_queryable() | schema() | list(schema()) | changeset() | list(changeset()),
    id_or_opts :: id() | list(id()) | opts()
  ) :: {:ok, list(schema())} | {:error, list(changeset())}
  def delete(%module{} = changeset, opts) when module === Ecto.Changeset do
    with {:error, %{data: %query{} = data} = changeset} <-
      changeset
      |> prepare_changeset(%{}, opts)
      |> Config.repo!(opts).delete(opts) do
      {:error, Error.call(:internal_server_error, "failed to delete record", %{
        changeset: changeset,
        schema_data: data,
        query: query
      })}
    end
  end

  def delete(%module{} = schema_data, opts) when module !== Ecto.Query do
    # when schema data is given it is wrapped in a changeset
    # so that a constraint error isn't raised and instead we
    # return a changeset error.
    with {:error, changeset} <-
      module
      |> prepare_changeset(schema_data, %{}, opts)
      |> Config.repo!(opts).delete(opts) do
      {:error, Error.call(:internal_server_error, "failed to delete record", %{
        changeset: changeset,
        schema_data: schema_data,
        query: module,
      })}
    end
  end

  def delete(delete_params_list, opts) when is_list(delete_params_list) do
    delete_params_list
    |> Enum.map(&delete(&1, opts))
    |> reduce_status_tuples()
  end

  def delete(query, id_or_params) do
    delete(query, id_or_params, default_opts())
  end

  @doc """
  Deletes a schema. Can also accept a keyword options list.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See `find/3` and [Ecto.Repo.delete/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.delete(YourSchema, 1)
      iex> EctoSchemas.Actions.delete(YourSchema, "binary_id")
      iex> EctoSchemas.Actions.delete(YourSchema, "binary_id", repo: YourApp.Repo)
      iex> EctoSchemas.Actions.delete({"source", YourSchema}, 1)
      iex> EctoSchemas.Actions.delete({"source", YourSchema}, "binary_id")
      iex> EctoSchemas.Actions.delete({"source", YourSchema}, "binary_id", repo: YourApp.Repo)
  """
  @spec delete(
    query :: query() | queryable() | source_queryable(),
    params :: id() | list(id()) | params() | list(params()),
    opts :: opts()
  ) :: {:ok, schema()} | {:error, changeset()}
  def delete(query, params_list, opts) when is_list(params_list) do
    params_list
    |> Enum.map(&delete(query, &1, opts))
    |> reduce_status_tuples()
  end

  def delete(query, id, opts) when (is_integer(id) or is_binary(id)) do
    with {:ok, schema_data} <- find(query, %{id: id}, opts) do
      delete(schema_data, opts)
    end
  end

  def delete(query, params, opts) do
    with {:ok, schema_data} <- find(query, params, opts) do
      delete(schema_data, opts)
    end
  end

  @doc """
  Returns a lazy enumerable that emits all entries matching the given query.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.stream/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:stream/2) for more options.

  ### Examples

      iex> EctoSchemas.Actions.stream(YourSchema, %{id: 1})
      iex> EctoSchemas.Actions.stream(YourSchema, %{id: 1}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.stream(YourSchema, %{id: 1}, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.stream({"source", YourSchema}, %{id: 1})
      iex> EctoSchemas.Actions.stream({"source", YourSchema}, %{id: 1}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.stream({"source", YourSchema}, %{id: 1}, replica: YourApp.Repo)
  """
  @spec stream(
    query :: query() | queryable() | source_queryable(),
    params :: params(),
    opts :: opts()
  ) :: list(schema())
  @spec stream(
    query :: query() | queryable() | source_queryable(),
    params :: params()
  ) :: list(schema())
  @spec stream(
    query :: query() | queryable() | source_queryable()
  ) :: list(schema())
  def stream(query, params \\ %{}, opts \\ []) do
    query
    |> CommonSchemas.get_schema_query()
    |> CommonFilters.convert_params_to_filter(params)
    |> Config.replica!(opts).stream(opts)
  end

  @doc """
  Calculate the given aggregate.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See [Ecto.Repo.aggregate/4](https://hexdocs.pm/ecto/Ecto.Repo.html#c:aggregate/4) for more options.

  ### Examples

      iex> EctoSchemas.Actions.aggregate(YourSchema, %{id: 1}, :count, :id)
      iex> EctoSchemas.Actions.aggregate(YourSchema, %{id: 1}, :count, :id, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.aggregate(YourSchema, %{id: 1}, :count, :id, replica: YourApp.Repo)
      iex> EctoSchemas.Actions.aggregate({"source", YourSchema}, %{id: 1}, :count, :id)
      iex> EctoSchemas.Actions.aggregate({"source", YourSchema}, %{id: 1}, :count, :id, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.aggregate({"source", YourSchema}, %{id: 1}, :count, :id, replica: YourApp.Repo)
  """
  @spec aggregate(
    query :: query() | queryable() | source_queryable(),
    params :: params(),
    aggregate :: aggregate_options(),
    field :: field(),
    opts :: opts()
  ) :: term() | nil
  @spec aggregate(
    query :: query() | queryable() | source_queryable(),
    params :: params(),
    aggregate :: aggregate_options(),
    field :: field()
  ) :: term() | nil
  def aggregate(query, params, aggregate, field, opts \\ []) do
    query
    |> CommonSchemas.get_schema_query()
    |> CommonFilters.convert_params_to_filter(params)
    |> Config.replica!(opts).aggregate(aggregate, field, opts)
  end

  @doc """
  A simple `Ecto.Repo` transaction wrapper.

  ### Options

    * `rollback_on_error` - When set to `true` if the function returns
      `{:error, term()}` or `:error` the transaction is rolled back,
      otherwise changes are committed. Defaults to `true`. This option
      does not apply when an `Ecto.Multi` is given as [Ecto.Repo.transaction/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:transaction/2-use-with-ecto-multi)
      will roll back the transaction if an error occurs.

  ### Examples

      iex> EctoShorts.Actions.transaction(fn -> :success end)
      {:ok, :success}
  """
  @doc since: "2.5.0"
  @spec transaction(
    fun_or_multi :: (-> any()) | (module() -> any()) | Ecto.Multi.t(),
    opts :: opts()
  ) :: {:ok, any()} | {:error, any()} | :error | Ecto.Multi.failure()
  @spec transaction(
    fun_or_multi :: (-> any()) | (module() -> any()) | Ecto.Multi.t()
  ) :: {:ok, any()} | {:error, any()} | :error | Ecto.Multi.failure()
  def transaction(fun_or_multi, opts \\ [])

  def transaction(%_{} = multi, opts) do
    Config.repo!(opts).transaction(multi, opts)
  end

  def transaction(fun, opts) do
    rollback_on_error? = Keyword.get(opts, :rollback_on_error, true)

    Config.repo!(opts).transaction(
      fn repo ->
        result = if is_function(fun, 1), do: fun.(repo), else: fun.()

        case result do
          {:error, _} = error ->
            if rollback_on_error?, do: repo.rollback(error), else: error

          :error ->
            if rollback_on_error?, do: repo.rollback(:error), else: :error

          response ->
            response

        end
      end,
      opts
    )
  end

  @doc """
  Finds a schema by params or creates one if it isn't found.
  Can also accept a keyword options list.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See `find/3` and `create/3` for more information.

  ### Examples

      iex> EctoSchemas.Actions.find_or_create(YourSchema, %{name: "great name"})
      iex> EctoSchemas.Actions.find_or_create(YourSchema, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find_or_create(YourSchema, %{name: "great name"}, replica: YourApp.Repo.replica())
      iex> EctoSchemas.Actions.find_or_create({"source", YourSchema}, %{name: "great name"})
      iex> EctoSchemas.Actions.find_or_create({"source", YourSchema}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find_or_create({"source", YourSchema}, %{name: "great name"}, replica: YourApp.Repo.replica())
  """
  @spec find_or_create(
    query :: queryable() | source_queryable(),
    params :: params(),
    opts :: opts()
  ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @spec find_or_create(
    query :: queryable() | source_queryable(),
    params :: params()
  ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def find_or_create(query, params, opts \\ []) do
    with {:error, %{code: :not_found}} <- find(query, params, opts) do
      query
      |> CommonSchemas.get_schema_queryable()
      |> create(params, opts)
    end
  end

  @doc """
  Finds a schema by params and updates it or creates with results of
  params/update_params merged. Can also accept a keyword options list.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See `find/3` and `update/4` for more information.

  ### Examples

      iex> EctoSchemas.Actions.find_and_update(YourSchema, %{id: 1}, %{name: "great name"})
      iex> EctoSchemas.Actions.find_and_update(YourSchema, %{id: 1}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find_and_update(YourSchema, %{id: 1}, %{name: "great name"}, replica: YourApp.Repo.replica())
      iex> EctoSchemas.Actions.find_and_update({"source", YourSchema}, %{id: 1}, %{name: "great name"})
      iex> EctoSchemas.Actions.find_and_update({"source", YourSchema}, %{id: 1}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find_and_update({"source", YourSchema}, %{id: 1}, %{name: "great name"}, replica: YourApp.Repo.replica())
  """
  @spec find_and_update(
    query :: queryable() | source_queryable(),
    find_params :: params(),
    update_params :: params(),
    opts :: opts()
  ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @spec find_and_update(
    query :: queryable() | source_queryable(),
    find_params :: params(),
    update_params :: params()
  ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def find_and_update(query, find_params, update_params, opts \\ []) do
    with {:ok, schema_data} <- find(query, find_params, opts) do
      query
      |> CommonSchemas.get_schema_queryable()
      |> update(schema_data, update_params, opts)
    end
  end

  @doc """
  Finds a schema by params and updates it or creates with results of
  params/update_params merged. Can also accept a keyword options list.

  ### Options

    * `:replica` - A module that uses `Ecto.Repo`. This option takes
      precedence over the `:repo` option and will be used to
      fetch the record if set.

    * `:repo` - A module that uses `Ecto.Repo`.

  See `find/3`, `create/3` and `update/4` for more information.

  ### Examples

      iex> EctoSchemas.Actions.find_and_upsert(YourSchema, %{id: 1}, %{name: "great name"})
      iex> EctoSchemas.Actions.find_and_upsert(YourSchema, %{id: 1}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find_and_upsert(YourSchema, %{id: 1}, %{name: "great name"}, replica: YourApp.Repo.replica())
      iex> EctoSchemas.Actions.find_and_upsert({"source", YourSchema}, %{id: 1}, %{name: "great name"})
      iex> EctoSchemas.Actions.find_and_upsert({"source", YourSchema}, %{id: 1}, %{name: "great name"}, repo: YourApp.Repo)
      iex> EctoSchemas.Actions.find_and_upsert({"source", YourSchema}, %{id: 1}, %{name: "great name"}, replica: YourApp.Repo.replica())
  """
  @spec find_and_upsert(
    query :: queryable() | source_queryable(),
    find_params :: params(),
    upsert_params :: params(),
    opts :: opts()
  ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @spec find_and_upsert(
    query :: queryable() | source_queryable(),
    find_params :: params(),
    upsert_params :: params()
  ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def find_and_upsert(query, find_params, upsert_params, opts \\ []) do
    case find(query, find_params, opts) do
      {:ok, schema_data} ->
        query
        |> CommonSchemas.get_schema_queryable()
        |> update(schema_data, upsert_params, opts)

      {:error, %{code: :not_found}} ->
        query
        |> CommonSchemas.get_schema_queryable()
        |> create(Map.merge(find_params, upsert_params), opts)

    end
  end

  @doc """
  ...
  """
  def find_or_create_many(query, params_list, opts) do
    params_list
    |> Stream.with_index()
    |> Enum.reduce(Ecto.Multi.new(), &reduce_multi_find_and_create(query, &1, &2, opts))
    |> Config.repo!(opts).transaction(opts)
  end

  def find_or_create_many(query, params_list) do
    find_or_create_many(query, params_list, default_opts())
  end

  defp reduce_multi_find_and_create(query, {params, i}, multi, opts) do
    Ecto.Multi.run(multi, i, fn repo, _changes ->
      params = put_order_by_and_group_by(params, opts)

      case query |> CommonFilters.convert_params_to_filter(params) |> repo.one(opts) do
        nil ->
          query
          |> CommonSchemas.get_schema_queryable()
          |> prepare_changeset(params, opts)
          |> repo.insert(opts)

        schema_data ->
          {:ok, schema_data}

      end
    end)
  end

  @doc """
  ...
  """
  @doc since: "2.5.0"
  def find_and_update_many(query, params_list, opts) do
    params_list
    |> Stream.with_index()
    |> Enum.reduce(Ecto.Multi.new(), &reduce_multi_find_and_update(query, &1, &2, opts))
    |> Config.repo!(opts).transaction(opts)
  end

  def find_and_update_many(query, params_list) do
    find_and_update_many(query, params_list, default_opts())
  end

  defp reduce_multi_find_and_update(query, {{find_params, update_params}, i}, multi, opts) do
    Ecto.Multi.run(multi, i, fn repo, _changes ->
      find_params = put_order_by_and_group_by(find_params, opts)

      case query |> CommonFilters.convert_params_to_filter(find_params) |> repo.one(opts) do
        nil ->
          {:error, Error.call(:not_found, "no records found", %{
            query: query,
            params: find_params
          })}

        schema_data ->
          schema_data
          |> prepare_changeset(update_params, opts)
          |> repo.update(opts)

      end
    end)
  end

  @doc """
  ...
  """
  @doc since: "2.5.0"
  def find_and_upsert_many(query, params_list, opts) do
    params_list
    |> Stream.with_index()
    |> Enum.reduce(Ecto.Multi.new(), &reduce_multi_find_and_upsert(query, &1, &2, opts))
    |> Config.repo!(opts).transaction(opts)
  end

  def find_and_upsert_many(query, params_list) do
    find_and_upsert_many(query, params_list, default_opts())
  end

  defp reduce_multi_find_and_upsert(query, {{find_params, upsert_params}, i}, multi, opts) do
    Ecto.Multi.run(multi, i, fn repo, _changes ->
      find_params = put_order_by_and_group_by(find_params, opts)

      case query |> CommonFilters.convert_params_to_filter(find_params) |> repo.one(opts) do
        nil ->
          query
          |> CommonSchemas.get_schema_queryable()
          |> prepare_changeset(upsert_params, opts)
          |> repo.insert(opts)

        schema_data ->
          schema_data
          |> prepare_changeset(upsert_params, opts)
          |> repo.update(opts)

      end
    end)
  end

  defp put_order_by_and_group_by(params, opts) do
    params
    |> put_order_by(opts)
    |> put_group_by(opts)
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

  defp prepare_changeset(%{data: %{__struct__: queryable}} = changeset, params, opts) do
    prepare_changeset(queryable, changeset, params, opts)
  end

  defp prepare_changeset(%{__struct__: queryable} = schema_data, params, opts) do
    prepare_changeset(queryable, schema_data, params, opts)
  end

  defp prepare_changeset({source, queryable}, params, opts) do
    schema_data = CommonSchemas.get_loaded_struct({source, queryable})

    prepare_changeset(queryable, schema_data, params, opts)
  end

  defp prepare_changeset(queryable, params, opts) do
    prepare_changeset(queryable, struct(queryable), params, opts)
  end

  defp prepare_changeset({source, queryable}, schema_data, params, opts) do
    # this re-writes the source on existing data
    schema_data = CommonSchemas.put_meta(schema_data, source: source)

    prepare_changeset(queryable, schema_data, params, opts)
  end

  defp prepare_changeset(queryable, model_or_changeset, params, opts) do
    case opts[:changeset] do
      nil ->
        queryable.changeset(model_or_changeset, params)

      {mod, fun, args} ->
        changeset = queryable.changeset(model_or_changeset, params)

        apply(mod, fun, [changeset] ++ args)

      func when is_function(func, 2) ->
        model_or_changeset
        |> queryable.changeset(params)
        |> func.(params)

      func when is_function(func, 1) ->
        model_or_changeset
        |> queryable.changeset(params)
        |> func.()

    end
  end

  # defp drop_associations(params, queryable) do
  #   Map.drop(params, queryable.__schema__(:associations))
  # end

  # defp merge_found(created_map, found_results) do
  #   created_map
  #   |> Enum.map(fn {index, result} -> {result, index} end)
  #   |> Kernel.++(found_results)
  #   |> Enum.sort(&(elem(&1, 1) >= elem(&2, 1)))
  #   |> Enum.map(&elem(&1, 0))
  # end

  defp reduce_status_tuples(status_tuples) do
    {oks, errors} =
      Enum.reduce(status_tuples, {[], []}, fn
        {:error, e}, {oks, errors} -> {oks, [e | errors]}
        {:ok, ok}, {oks, errors} -> {[ok | oks], errors}
      end)

    case {oks, errors} do
      {oks, []} -> {:ok, Enum.reverse(oks)}
      {_, errors} -> {:error, Enum.reverse(errors)}
    end
  end

  defp default_opts do
    [repo: Config.repo(), replica: Config.replica()]
  end
end
