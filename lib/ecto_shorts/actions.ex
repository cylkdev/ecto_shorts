defmodule EctoShorts.Actions do
  @moduledoc """
  Provides a standardized API for simplifying Ecto repo operations
  and reducing boilerplate.

  This module offers a declarative, parameter-driven approach to
  building queries and handling data operations. It abstracts away
  many of the common pitfalls of working with Ecto—so you can focus
  on what your code should do, not how to make Ecto do it.

  ## Shared Options

  The following options are accepted by almost all functions in this module:

    * `:replica` – Specifies the repo module to use for read operations.
      Takes precedence over the `:repo` option when set.

    * `:repo` – Specifies the repo module to use for both read and write
      operations.
  """

  alias EctoShorts.{
    Actions.Error,
    CommonFilters,
    CommonParams,
    CommonSchemas,
    Config,
    CommonQueries,
    Utils
  }

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type changeset :: Ecto.Changeset.t()

  @type schema_data :: Ecto.Schema.t()

  @type aggregate_options :: :avg | :count | :max | :min | :sum

  @type multi :: Ecto.Multi.t()

  @type multi_failure :: Ecto.Multi.failure()

  @type id :: integer() | binary()

  @type batch_key :: atom()

  @type batch_id :: map()

  @type params :: map()

  @type opts :: keyword()

  @type stream :: %Stream{}

  @doc group: "Schema API"
  @doc """
  Inserts multiple records into the database.

  ## Data Validation

  By default, each entry in `params_list` is validated using the schema’s
  `changeset/2` function. If any entry fails validation, the operation is
  halted and the corresponding errors are returned.

  If this behaviour is not desired you can set the option `:validate` to
  `false`.

  ## Conflict Handling

  When a complete primary key is present in an entry, this function
  treats it as an upsert. It automatically sets:

    - `:conflict_target` to the schema’s primary key

    - `:on_conflict` to `{:replace, keys}`

  Where `keys` are all queryable fields on the schema, excluding the
  primary key and the `:inserted_at` timestamp field.

  This ensures existing records are updated cleanly with minimal setup.

  ## Preloading

  To fetch existing records before inserting, enable the `:batch_preload`
  option.

  If an entry is a map with a complete primary key, the function will:

    - Fetch the existing record from the database

    - Replace the map with a `{schema_data(), params()}` tuple

  If the entry is already a tuple `{find_params, params}`, and the
  `find_params` contain the complete primary key, it will:

    - Query the existing record using `find_params`

    - Return `{schema_data(), params()}`

  This allows you to preload existing rows and apply new params against
  them before validation.

  See `batch_preload/4` for more details.

  ## Options

    * `:repo` – Specifies the repo module to use for both read and write
      operations.

    * `:batch_preload` - Can be the atom `:primary_key` or a list of
      keys used to determine the uniqueness of of each record in the
      set.

  See `EctoShorts.CommonParams.convert_to_insert_all_params/3` for more options.

  ## Examples

      iex> EctoShorts.Actions.insert_all(Post, [%{title: "post_title"}], returning: true)
      {:ok, {1, [%Post{title: "post_title"}]}}
  """
  @spec insert_all(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(any())
        ) :: {:ok, {non_neg_integer(), nil | [term()]}} | {:error, any()}
  @spec insert_all(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(any()),
          opts :: opts()
        ) :: {:ok, {non_neg_integer(), nil | [term()]}} | {:error, any()}
  def insert_all(query, params_list, opts \\ []) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    with {:ok, inserts} <-
           CommonParams.convert_to_insert_all_params(
             schema_module,
             maybe_batch_preload(query, params_list, opts),
             opts
           ) do
      opts =
        if CommonParams.any_has_primary_key?(schema_module, inserts) do
          opts
          |> CommonParams.on_conflict_options(schema_module)
          |> Keyword.merge(opts)
        else
          opts
        end

      {:ok, Config.repo!(opts).insert_all(schema_module, inserts, opts)}
    end
  end

  defp maybe_batch_preload(query, params_list, opts) do
    if Keyword.has_key?(opts, :batch_preload) do
      batch_preload(query, opts[:batch_preload] || :primary_key, params_list, opts)
    else
      params_list
    end
  end

  @doc """
  ...
  """
  def batch_preload(query, batch_key \\ :primary_key, params_list, opts \\ []) do
    case take_batch_preload_params(params_list, batch_key, query) do
      [] ->
        params_list

      todo ->
        query
        |> batch(batch_key, todo, opts)
        |> put_batch_preload_results(params_list, batch_key, query)
    end
  end

  defp put_batch_preload_results(batch_results, params_list, batch_key, query) do
    Enum.map(params_list, fn
      {find_params, params} when is_map(find_params) and not is_struct(find_params) ->
        if has_batch_key?(find_params, batch_key, query) do
          case Map.get(batch_results, fetch_batch_id!(find_params, batch_key, query)) do
            nil -> {find_params, params}
            schema_data -> {schema_data, drop_batch_keys(params, batch_key, query)}
          end
        else
          params
        end

      params when is_map(params) and not is_struct(params) ->
        if has_batch_key?(params, batch_key, query) do
          case Map.get(batch_results, fetch_batch_id!(params, batch_key, query)) do
            nil -> params
            schema_data -> {schema_data, drop_batch_keys(params, batch_key, query)}
          end
        else
          params
        end

      term ->
        term
    end)
  end

  defp drop_batch_keys(params, batch_key, query) do
    case batch_key do
      :primary_key -> Map.drop(params, CommonSchemas.get_schema_reflection(query, :primary_key))
      keys -> Map.drop(params, keys)
    end
  end

  defp take_batch_preload_params(params_list, batch_key, query) do
    Enum.reduce(params_list, [], fn
      {find_params, _params}, acc when is_map(find_params) and not is_struct(find_params) ->
        if has_batch_key?(find_params, batch_key, query) do
          [find_params | acc]
        else
          acc
        end

      params, acc when is_map(params) and not is_struct(params) ->
        if has_batch_key?(params, batch_key, query) do
          [params | acc]
        else
          acc
        end

      _, acc ->
        acc
    end)
  end

  @doc group: "Batch API"
  @doc """
  Finds all records matching the given parameter sets.

  Each map in the provided list of parameters is used to build an `OR`
  condition. This allows you to find all records where any parameter set
  matches, without manually constructing complex OR queries.

  For example, to find users who match any of the following:

    - Has the email "user1@example.com"
    - Has the name "User 2"
    - Belongs to organization 3

  You can pass:

  ```elixir
  [
    %{email: "user1@example.com"},
    %{name: "User 2"},
    %{org_id: 3}
  ]
  ```

  Instead of writing multiple queries, a single query will return all
  matching records.

  ## Composite keys

  For cases where a single lookup requires matching multiple fields
  (composite keys), see `batch/4` for details.

  ## Options

    * `:replica` – Specifies the repo module to use for read operations.
      Takes precedence over the `:repo` option when set.

    * `:repo` – Specifies the repo module to use for both read and write
      operations.

  ## Examples

      iex> Actions.batch_find(Post, [:title], [%{title: "post_title"}])
      {:ok, [%Post{title: "post_title"}]}
  """
  @spec batch_find(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec batch_find(
          query :: query() | queryable() | source_queryable(),
          batch_key :: :primary_key | batch_key() | list(batch_key()),
          params_list :: list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec batch_find(
          query :: query() | queryable() | source_queryable(),
          batch_key :: :primary_key | batch_key() | list(batch_key()),
          params_list :: list(params()),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def batch_find(query, batch_key \\ :primary_key, params_list, opts \\ []) do
    batch_results = batch(query, batch_key, params_list, opts)

    params_list
    |> Stream.with_index()
    |> Utils.reduce_all(fn {params, i} ->
      case Map.get(batch_results, fetch_batch_id!(params, batch_key, query)) do
        nil ->
          {:error,
           Error.call(:not_found, "Record not found.", %{
             query: query,
             params: params_list,
             failed_value: params,
             position: i,
             batch_key: normalize_batch_key(batch_key, query)
           })}

        schema_data ->
          {:ok, schema_data}
      end
    end)
  end

  @doc group: "Batch API"
  @doc """
  Returns records as a map where the keys are the batch parameters and
  values are the matching records.

  When querying records, you often need to maintain the relationship
  between your search parameters and the records they matched. This function
  helps by returning a map where each record is keyed by the parameters used to find it.

  For example, if you need to find posts and keep track of which title matched each post:

  ```elixir
  iex> EctoShorts.Actions.batch(Post, [:title], [
  ...>   %{title: "First Post"},
  ...>   %{title: "Second Post"}
  ...> ])
  ```

  This function will return a map like this:

  ```elixir
  %{
    %{title: "First Post"} => %Post{title: "First Post"},
    %{title: "Second Post"} => %Post{title: "Second Post"}
  }
  ```

  ## Composite keys

  The function also supports searching by composite keys. This is useful
  when you need to match records using multiple fields together.

  For instance, finding users in specific organizations:

  ```elixir
  iex> EctoShorts.Actions.batch(User, [:organization_id, :email], [
  ...>   %{organization_id: 1, email: "user_one@email.com"},
  ...>   %{organization_id: 2, email: "user_two@email.com"}
  ...> ])
  ```

  This function will return a map like this:

  ```elixir
  %{
    %{organization_id: 1, email: "user_one@email.com"} => %User{id: 1, organization_id: 1, email: "user_one@my-app.com"},
    %{organization_id: 2, email: "user_two@email.com"} => %User{id: 2, organization_id: 2, email: "user_two@my-app.com"}
  }
  ```

  If no batch keys are provided, the function will automatically use the
  schema's primary keys. For example, if your schema has a composite primary
  key of `:org_id` and `:user_id`, you don't need to specify these as batch
  keys as they will be used by default.

  This function will raise a `KeyError` if the schema disables primary keys
  and a batch key is not provided.

  ## Streaming

  The batch function supports streaming results when the `:stream` option
  is provided. This is particularly useful when dealing with large datasets
  to manage memory usage.

  For example:

  ```elixir
  iex> EctoShorts.Repo.transaction(fn ->
  ...>   Post
  ...>   |> Actions.batch([:title], [%{title: "post_title"}], stream: true)
  ...>   |> Enum.to_list()
  ...> end)
  {:ok, [%{%{title: "post_title"} => %Post{title: "post_title"}}]}
  ```

  ## Examples

      iex> Actions.batch(Post, [:title], [%{title: "post_title"}])
      {:ok, %Post{title: "title"}}
  """
  @spec batch(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(params())
        ) :: %{batch_id() => schema_data()}
  @spec batch(
          query :: query() | queryable() | source_queryable(),
          batch_key :: :primary_key | batch_key() | list(batch_key()),
          params_list :: list(params())
        ) :: %{batch_id() => schema_data()}
  @spec batch(
          query :: query() | queryable() | source_queryable(),
          batch_key :: :primary_key | batch_key() | list(batch_key()),
          params_list :: list(params()),
          opts :: opts()
        ) :: %{batch_id() => schema_data()}
  def batch(query, batch_key \\ :primary_key, params_list, opts \\ []) do
    query
    |> all(%{or_where: params_list}, opts)
    |> Enum.group_by(&fetch_batch_id!(&1, batch_key, query))
    |> Map.new(fn
      {batch_id, []} ->
        {batch_id, []}

      {batch_id, [schema_data]} ->
        {batch_id, schema_data}

      {batch_id, schema_datas} ->
        raise "Expected one record for batch keys #{inspect(Map.keys(batch_id))}, got #{length(schema_datas)}"
    end)
  end

  defp fetch_batch_id!(data, primary_key_or_keys, query) do
    with :error <- fetch_batch_id(data, primary_key_or_keys, query) do
      raise "Batch key not found for schema #{CommonSchemas.get_schema_queryable(query)}: #{inspect(data)}"
    end
  end

  defp fetch_batch_id(data, primary_key_or_keys, query) do
    case normalize_batch_key(primary_key_or_keys, query) do
      [] -> :error
      keys -> fetch_keys(data, keys)
    end
  end

  defp has_batch_key?(data, value, query) do
    case fetch_keys(data, normalize_batch_key(value, query)) do
      :error -> false
      _ -> true
    end
  end

  defp fetch_keys(data, keys) do
    result =
      Utils.reduce_all(keys, fn key ->
        case Map.get(data, key) do
          nil -> {:error, key}
          value -> {:ok, {key, value}}
        end
      end)

    case result do
      {:ok, values} -> Map.new(values)
      _ -> :error
    end
  end

  defp normalize_batch_key(:primary_key, query) do
    CommonSchemas.get_schema_reflection(query, :primary_key)
  end

  defp normalize_batch_key(key, _query) do
    List.wrap(key)
  end

  @doc group: "Schema API"
  @doc """
  ...
  """
  @spec update_all(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          update_params :: params()
        ) :: {non_neg_integer(), nil | [term()]}
  @spec update_all(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          update_params :: params(),
          opts :: opts()
        ) :: {non_neg_integer(), nil | [term()]}
  def update_all(query, find_params, update_params, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    updates =
      query
      |> CommonSchemas.get_schema_queryable()
      |> CommonParams.convert_to_update_all_params(update_params, opts)

    query
    |> CommonFilters.convert_params_to_filter(find_params, opts)
    |> Config.repo!(opts).update_all(updates, opts)
  end

  @doc group: "Query API"
  @doc """
  ...
  """
  @spec delete_all(
          query :: query() | queryable() | source_queryable(),
          params :: params()
        ) :: {non_neg_integer(), nil | [term()]}
  @spec delete_all(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          opts :: opts()
        ) :: {non_neg_integer(), nil | [term()]}
  def delete_all(query, params, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end

  @doc group: "Multi API"
  @doc """
  Retrieves a record matching the given parameters or creates
  a record in sequential order inside a transaction.

  ### Examples

      iex> SchemasPG.Actions.find_or_create_many(MyApp.User, [%{id: 1, username: "fira", full_name: "Fira"}])
  """
  @spec find_or_create_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(map() | {map(), map()})
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_or_create_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(map() | {map(), map()}),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def find_or_create_many(query, params_list, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> multi_find_or_create(params_list, opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_find_or_create(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, i}, multi ->
      Ecto.Multi.run(multi, {:find_or_create, i}, fn repo, _changes_so_far ->
        case query |> CommonFilters.convert_params_to_filter(params, opts) |> repo.one(opts) do
          nil ->
            with {:error, changeset} <-
                   query
                   |> create_changeset(params, opts)
                   |> repo.insert(opts) do
              {:error,
               {:conflict, "Failed to create record.",
                %{
                  query: query,
                  params: params_list,
                  changeset: changeset,
                  position: i,
                  keys: params
                }}}
            end

          schema_data ->
            {:ok, schema_data}
        end
      end)
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...
  """
  @spec find_and_update_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(map() | {map(), map()})
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_and_update_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(map() | {map(), map()}),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def find_and_update_many(query, params_list, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> multi_find_and_update(params_list, opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_find_and_update(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {args, i}, multi ->
      {find_params, update_params} = unzip_find_params(args, query, opts)

      Ecto.Multi.run(multi, {:find_and_update, i}, fn repo, _changes_so_far ->
        case query
             |> CommonFilters.convert_params_to_filter(find_params, opts)
             |> repo.one(opts) do
          nil ->
            {:error,
             {:not_found, "Record not found.",
              %{
                query: query,
                position: i,
                params: params_list,
                failing_value: find_params
              }}}

          schema_data ->
            with {:error, changeset} <-
                   query
                   |> CommonSchemas.prepare_changeset(
                     schema_data,
                     Map.merge(find_params, update_params),
                     opts
                   )
                   |> repo.update(opts) do
              {:error,
               {:conflict, "Failed to update record.",
                %{
                  query: query,
                  position: 1,
                  changeset: changeset,
                  params: params_list
                }}}
            end
        end
      end)
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...
  """
  @spec find_and_upsert_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(map() | {map(), map()})
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_and_upsert_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(map() | {map(), map()}),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def find_and_upsert_many(query, params_list, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> multi_find_and_upsert(params_list, opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_find_and_upsert(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {args, i}, multi ->
      {find_params, upsert_params} = unzip_find_params(args, query, opts)

      Ecto.Multi.run(multi, {:find_and_upsert, i}, fn repo, _changes_so_far ->
        case query
             |> CommonFilters.convert_params_to_filter(find_params, opts)
             |> repo.one(opts) do
          nil ->
            with {:error, changeset} <-
                   query
                   |> create_changeset(Map.merge(find_params, upsert_params), opts)
                   |> repo.insert(opts) do
              {:error,
               {:conflict, "Failed to create record.",
                %{
                  query: query,
                  position: 1,
                  changeset: changeset,
                  params: params_list
                }}}
            end

          schema_data ->
            with {:error, changeset} <-
                   query
                   |> CommonSchemas.prepare_changeset(
                     schema_data,
                     Map.merge(find_params, upsert_params),
                     opts
                   )
                   |> repo.update(opts) do
              {:error,
               {:conflict, "Failed to update record.",
                %{
                  query: query,
                  position: 1,
                  changeset: changeset,
                  params: params_list
                }}}
            end
        end
      end)
    end)
  end

  defp unzip_find_params({find_params, params}, _query, _opts) do
    {find_params, params}
  end

  defp unzip_find_params(params, query, opts) do
    {maybe_drop_associations(params, query, opts), params}
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  Creates many records sequentially in a transaction.

  ### Examples

      iex> SchemasPG.Actions.create_many(MyApp.User, [%{username: "fira", full_name: "Fira"}])
  """
  @spec create_many(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec create_many(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(params()),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def create_many(query, params_list, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> multi_insert(params_list, opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_insert(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, i}, multi ->
      Ecto.Multi.run(multi, {:create, i}, fn repo, _changes_so_far ->
        with {:error, changeset} <-
               query
               |> CommonSchemas.prepare_changeset(params, opts)
               |> repo.insert(opts) do
          {:error,
           {:conflict, "Failed to create record.",
            %{
              query: query,
              position: 1,
              changeset: changeset,
              params: params_list
            }}}
        end
      end)
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  Retrieves many records matching the parameters sequentially in a transaction.

  ### Examples

      iex> SchemasPG.Actions.find_many(MyApp.User, [%{username: "fira"}])
  """
  @spec find_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_many(
          query :: query() | queryable() | source_queryable(),
          params_list :: list(params()),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def find_many(query, params_list, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> multi_find(params_list, opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_find(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, i}, multi ->
      Ecto.Multi.run(multi, {:find, i}, fn repo, _changes_so_far ->
        case query
             |> CommonFilters.convert_params_to_filter(params, opts)
             |> repo.one(opts) do
          nil ->
            {:error,
             {:not_found, "Record not found.",
              %{
                query: query,
                position: i,
                failing_value: params,
                params: params_list
              }}}

          schema_data ->
            {:ok, schema_data}
        end
      end)
    end)
  end

  @doc group: "Multi API"
  @doc """
  ...
  """
  @spec delete_many(schema_data_or_changesets :: list(schema_data() | changeset())) ::
          {:ok, list(schema_data())} | {:error, any()}
  @spec delete_many(
          schema_data_or_changesets :: list(schema_data() | changeset()),
          opts :: opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def delete_many(schema_data_or_changesets, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    schema_data_or_changesets
    |> multi_delete(opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_delete(schema_data_or_changesets, opts) do
    schema_data_or_changesets
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {struct_or_changeset, i}, multi ->
      Ecto.Multi.run(multi, {:create, i}, fn repo, _changes_so_far ->
        with {:error, changeset} <-
               struct_or_changeset
               |> CommonSchemas.prepare_changeset(%{}, opts)
               |> repo.delete(opts) do
          {:error,
           {:conflict, "Failed to delete record.",
            %{
              query: CommonSchemas.get_schema_metadata(struct_or_changeset).schema,
              position: 1,
              changeset: changeset,
              params: schema_data_or_changesets
            }}}
        end
      end)
    end)
  end

  defp handle_multi_response(
         {:error, _failed_operation, {code, message, details}, changes_so_far}
       ) do
    {:error,
     Error.call(
       code,
       message,
       Map.put(details, :changes_so_far, Map.values(changes_so_far))
     )}
  end

  defp handle_multi_response({:ok, operations}) do
    {:ok, Map.values(operations)}
  end

  @doc """
  Finds a record or creates one if not found using distinct parameters.

  This is a more flexible version of `find_or_create/3`. Instead of using the same
  parameters for both the find and create operations, this function allows you to
  provide separate values:

    * `find_params` — used strictly for querying an existing record
    * `create_params` — used only if no matching record is found

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  For more information, see `&find/3` and `&create/3`.

  ## Examples

    iex> EctoShorts.Actions.find_and_create(MyApp.User, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo)
    iex> EctoShorts.Actions.find_and_create(MyApp.User, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, replica: MyApp.Repo.Replica)
    iex> EctoShorts.Actions.find_and_create(MyApp.User, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo, replica: MyApp.Repo.Replica)

    iex> EctoShorts.Actions.find_and_create({"users", MyApp.User}, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo)
    iex> EctoShorts.Actions.find_and_create({"users", MyApp.User}, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, replica: MyApp.Repo.Replica)
    iex> EctoShorts.Actions.find_and_create({"users", MyApp.User}, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo, replica: MyApp.Repo.Replica)
  """
  @spec find_and_create(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          create_params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_create(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          create_params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_create(query, find_params, create_params, opts \\ []) do
    with {:error, %{code: :not_found}} <- find(query, find_params, opts) do
      query
      |> CommonQueries.get_query_source()
      |> create(create_params, opts)
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

      iex> EctoShorts.Actions.find_and_update(MyApp.User, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_update(MyApp.User, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_update(MyApp.User, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_and_update({"users", MyApp.User}, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_update({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_update({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_and_update(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          update_params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_update(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          update_params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_update(query, find_params, update_params, opts \\ []) do
    with {:ok, struct} <- find(query, find_params, opts) do
      query
      |> CommonQueries.get_query_source()
      |> update(struct, update_params, opts)
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

      iex> EctoShorts.Actions.find_and_upsert(MyApp.User, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_upsert(MyApp.User, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_upsert(MyApp.User, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_and_upsert({"users", MyApp.User}, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_upsert({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_upsert({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_and_upsert(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          update_params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_upsert(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          update_params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_upsert(query, find_params, update_params, opts \\ []) do
    case find(query, find_params, opts) do
      {:ok, struct} ->
        update(query, struct, update_params, opts)

      {:error, %{code: :not_found}} ->
        create(query, Map.merge(find_params, update_params), opts)

      {:error, _} = e ->
        e
    end
  end

  @doc group: "Query API"
  @doc """
  ...
  """
  @spec find_and_delete(
          query :: query() | queryable() | source_queryable(),
          find_params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_delete(
          query :: query() | queryable() | source_queryable(),
          find_params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_delete(query, find_params, opts \\ []) do
    with {:ok, struct} <- find(query, find_params, opts) do
      delete(struct, opts)
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

  iex> EctoShorts.Actions.find_or_create(MyApp.User, %{name: "Fira"}, repo: MyApp.Repo)
  iex> EctoShorts.Actions.find_or_create(MyApp.User, %{name: "Fira"}, replica: MyApp.Repo.Replica)

  iex> EctoShorts.Actions.find_or_create({"users", MyApp.User}, %{name: "Fira"}, repo: MyApp.Repo)
  iex> EctoShorts.Actions.find_or_create({"users", MyApp.User}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_or_create(
          query :: query() | queryable() | source_queryable(),
          params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_or_create(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_or_create(query, params, opts \\ []) do
    with {:error, %{code: :not_found}} <-
           find(query, maybe_drop_associations(params, query, opts), opts) do
      query
      |> CommonQueries.get_query_source()
      |> create(params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Retrieves a single record by it's primary key.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.get/3`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) are also supported.

  ## Examples

      iex> EctoShorts.Actions.get(MyApp.User, 1)
      iex> EctoShorts.Actions.get(MyApp.User, 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.get(MyApp.User, 1, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.get({"users", MyApp.User}, 1)
      iex> EctoShorts.Actions.get({"users", MyApp.User}, 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.get({"users", MyApp.User}, 1, replica: MyApp.Repo.Replica)
  """
  @spec get(
          query :: query() | queryable() | source_queryable(),
          id :: id()
        ) :: schema_data() | nil
  @spec get(
          query :: query() | queryable() | source_queryable(),
          id :: id(),
          opts :: opts()
        ) :: schema_data() | nil
  def get(query, id, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    Config.replica!(opts).get(query, id, opts)
  end

  @doc group: "Query API"
  @doc """
  A shorthand for `&all/3`, allowing for filters and options to be passed more conveniently.

  This function accepts either a map or a keyword list as the second argument:

  When a map is given it is treated as filter parameters and are applied to the query.

  When a keyword list is given it can be used to pass options such as `:repo` or `:replica`
  and all other keys are treated as filter parameters and are applied to the query.

  ## Options

  See `&all/3` for options.

  ## Examples

      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1})
      iex> EctoShorts.Actions.all(MyApp.User, id: 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all(MyApp.User, id: 1, replica: MyApp.Repo)

      iex> EctoShorts.Actions.all({"users", MyApp.User}, %{id: 1})
      iex> EctoShorts.Actions.all({"users", MyApp.User}, id: 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all({"users", MyApp.User}, id: 1, replica: MyApp.Repo)
  """
  @spec all(query :: query() | queryable() | source_queryable()) :: list(schema_data())
  @spec all(query :: query() | queryable() | source_queryable(), params :: params()) ::
          list(schema_data())
  @spec all(query :: query() | queryable() | source_queryable(), opts :: opts()) ::
          list(schema_data())
  def all(query, params_or_opts \\ [])

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

      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1})
      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1}, prefix: "public")
      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1}, replica: MyApp.Repo)

      iex> EctoShorts.Actions.all({"users", MyApp.User}, %{id: 1})
      iex> EctoShorts.Actions.all({"users", MyApp.User}, %{id: 1}, prefix: "public")
      iex> EctoShorts.Actions.all({"users", MyApp.User}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all({"users", MyApp.User}, replica: MyApp.Repo)
  """
  @spec all(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          opts :: opts()
        ) :: list(schema_data())
  def all(query, params, opts) do
    opts = Keyword.merge(default_opts(), opts)

    params =
      params
      |> put_order_by(opts)
      |> put_group_by(opts)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).all(opts)
  end

  @doc group: "Schema API"
  @doc """
  Inserts a new record.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.insert/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.create(MyApp.User, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.create({"users", MyApp.User}, %{name: "Fira"}, repo: MyApp.Repo)
  """
  @spec create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def create(query, params, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
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

      iex> EctoShorts.Actions.find(MyApp.User, %{id: 1}, replica: MyApp.Repo)
      iex> EctoShorts.Actions.find({"users", MyApp.User}, %{id: 1}, replica: MyApp.Repo)
  """
  @spec find(
          query :: query() | queryable() | source_queryable(),
          params :: params()
        ) :: {:ok, schema_data()} | {:error, any()}
  @spec find(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, any()}
  def find(query, params, opts \\ [])

  def find(query, params, opts) when params === %{} do
    {:error,
     Error.call(
       :not_found,
       "Record not found.",
       %{
         query: query,
         params: params
       },
       opts
     )}
  end

  def find(query, params, opts) do
    opts = Keyword.merge(default_opts(), opts)

    params =
      params
      |> put_order_by(opts)
      |> put_group_by(opts)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    result =
      query
      |> CommonFilters.convert_params_to_filter(params, opts)
      |> Config.replica!(opts).one(opts)

    case result do
      nil ->
        {:error,
         Error.call(
           :not_found,
           "Record not found.",
           %{
             query: query,
             params: params
           },
           opts
         )}

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

      iex> EctoShorts.Actions.update(MyApp.User, %MyApp.User{id: 1, body: "old"}, %{username: "fira"})
      iex> EctoShorts.Actions.update({"users", MyApp.User}, %MyApp.User{id: 1, body: "old"}, %{username: "fira"})
  """
  @spec update(
          query :: query() | queryable() | source_queryable(),
          id_or_struct :: integer() | binary() | schema_data(),
          update_params :: params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec update(
          query :: query() | queryable() | source_queryable(),
          id_or_struct :: integer() | binary() | schema_data(),
          update_params :: params(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def update(query, id_or_struct, update_params, opts \\ [])

  def update(query, id, update_params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, struct} <- find(query, %{id: id}, opts) do
      query
      |> CommonSchemas.get_schema_queryable()
      |> update(struct, update_params, opts)
    end
  end

  def update(query, struct, update_params, opts) when is_list(update_params) do
    update(query, struct, Map.new(update_params), opts)
  end

  def update(query, struct, update_params, opts) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonSchemas.prepare_changeset(struct, update_params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc """
  Deletes a struct.

  This function supports the following arguments:

    * `(query, id)` — where `query` is an Ecto schema module or queryable,
      and `id` is the primary key.

    * `(changeset, opts)` — where `changeset` is an `Ecto.Changeset`, and
      `opts` is a keyword list of options.

    * `(struct, opts)` — where `struct` is a loaded schema
      struct, and `opts` is a keyword list of options.

  When a list of parameters are specified they are deleted in order
  and are not wrapped in a transaction.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [`Ecto.Repo.delete/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.create(MyApp.User, 1)
      iex> EctoShorts.Actions.delete(%Ecto.Changeset{data: %MyApp.User{}})
      iex> EctoShorts.Actions.delete(%MyApp.User{})
      iex> EctoShorts.Actions.create(%MyApp.User{}, repo: MyApp.Repo)
  """
  @spec delete(
          schema_data_or_changesets ::
            schema_data() | changeset() | list(schema_data() | changeset())
        ) :: {:ok, list(schema_data())} | {:error, list(changeset())} | {:error, any()}
  @spec delete(
          struct_or_changeset ::
            schema_data() | changeset() | list(schema_data() | changeset()),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def delete(schema_data_or_changesets, opts \\ [])

  def delete(%_{data: %_{__meta__: %{schema: queryable}}} = changeset, opts) do
    opts = Keyword.merge(default_opts(), opts)

    with {:error, changeset} <-
           queryable
           |> CommonSchemas.prepare_changeset(changeset, %{}, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "Failed to delete record.",
         %{
           query: queryable,
           changeset: changeset,
           schema_data: changeset.data
         },
         opts
       )}
    end
  end

  def delete(%_{__meta__: %{schema: queryable}} = struct, opts) do
    opts = Keyword.merge(default_opts(), opts)

    with {:error, changeset} <-
           queryable
           |> CommonSchemas.prepare_changeset(struct, %{}, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "Failed to delete record.",
         %{
           query: queryable,
           changeset: changeset,
           schema_data: struct
         },
         opts
       )}
    end
  end

  def delete(schema_data_or_changesets, opts) when is_list(schema_data_or_changesets) do
    Utils.reduce_all(schema_data_or_changesets, fn struct_or_changeset ->
      delete(struct_or_changeset, opts)
    end)
  end

  def delete(query, id) when is_binary(id) or is_integer(id) do
    delete(query, id, [])
  end

  @doc """
  Deletes a record matching the given `id`.

  ## Options

  See `delete/2` for options.

  ## Examples

      iex> EctoShorts.Actions.delete(MyApp.User, 1)
      iex> EctoShorts.Actions.delete(MyApp.User, "binary_id")
      iex> EctoShorts.Actions.delete(MyApp.User, "binary_id", repo: MyApp.Repo)

      iex> EctoShorts.Actions.delete({"users", MyApp.User}, 1)
      iex> EctoShorts.Actions.delete({"users", MyApp.User}, "binary_id")
      iex> EctoShorts.Actions.delete({"users", MyApp.User}, "binary_id", repo: MyApp.Repo)
  """
  @spec delete(
          query :: query() | queryable() | source_queryable(),
          id :: id(),
          opts :: opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def delete(query, id, opts) when is_integer(id) or is_binary(id) do
    with {:ok, struct} <- find(query, %{id: id}, opts) do
      delete(struct, opts)
    end
  end

  @doc """
  Returns a lazy enumerable that emits all entries matching the given query.

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [Ecto.Repo.stream/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:stream/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.stream(MyApp.User, %{id: 1}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.stream(MyApp.User, %{id: 1}, replica: MyApp.Repo)

      iex> EctoShorts.Actions.stream({"users", MyApp.User}, %{id: 1}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.stream({"users", MyApp.User}, %{id: 1}, replica: MyApp.Repo)
  """
  @spec stream(query :: query() | queryable() | source_queryable()) :: stream()
  @spec stream(query :: query() | queryable() | source_queryable(), params :: params()) ::
          stream()
  @spec stream(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          opts :: opts()
        ) :: stream()
  def stream(query, params \\ %{}, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc """
  Calculates an aggregate value for `field`, using the specified `aggregate` function.

  This function builds a query from the given schema or queryable, applies filters from `params`,
  and calculates the aggregate (e.g. `:count`, `:sum`, etc.) on the specified field.

  ## Supported Aggregates

  This function supports all ecto repo aggregate types, for example:

    * `:count` – Counts the number of matching rows.

    * `:sum` – Returns the sum of all values in the field.

    * `:avg` – Returns the average of the values in the field.

    * `:min` – Returns the minimum value in the field.

    * `:max` – Returns the maximum value in the field

  ## Options

  This function supports the [Shared Options](EctoShorts.Actions.html#module-shared-options) in the module docs.

  All options accepted by [Ecto.Repo.aggregate/4](https://hexdocs.pm/ecto/Ecto.Repo.html#c:aggregate/4) are also supported.

  ## Examples

      iex> EctoShorts.Actions.aggregate(MyApp.User, %{id: 1}, :count, :id)
      iex> EctoShorts.Actions.aggregate(MyApp.User, %{id: 1}, :count, :id, repo: MyApp.Repo)
      iex> EctoShorts.Actions.aggregate(MyApp.User, %{id: 1}, :count, :id, replica: MyApp.Repo)

      iex> EctoShorts.Actions.aggregate({"users", MyApp.User}, %{id: 1}, :count, :id)
      iex> EctoShorts.Actions.aggregate({"users", MyApp.User}, %{id: 1}, :count, :id, repo: MyApp.Repo)
      iex> EctoShorts.Actions.aggregate({"users", MyApp.User}, %{id: 1}, :count, :id, replica: MyApp.Repo)
  """
  @spec aggregate(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          aggregate :: aggregate_options(),
          field :: atom()
        ) :: any() | nil
  @spec aggregate(
          query :: query() | queryable() | source_queryable(),
          params :: params(),
          aggregate :: aggregate_options(),
          field :: atom(),
          opts :: opts()
        ) :: any() | nil
  def aggregate(query, params, aggregate, field, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, field, opts)
  end

  @doc group: "Transaction API"
  @doc since: "2.5.0"
  @doc """
  Runs the given function or multi inside a transaction.

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

      iex> EctoShorts.Actions.transaction(MyApp.User, fn -> :ok end, repo: MyApp.Repo)
      iex> EctoShorts.Actions.transaction(MyApp.User, fn -> :error end, rollback_on_error: false)

      iex> EctoShorts.Actions.transaction({"users", MyApp.User}, fn -> :ok end, repo: MyApp.Repo)
      iex> EctoShorts.Actions.transaction({"users", MyApp.User}, fn -> :error end, rollback_on_error: false)
  """
  @spec transaction(fun_or_multi :: function() | multi()) ::
          {:ok, any()} | {:error, any()} | multi_failure()
  @spec transaction(
          fun_or_multi :: function() | multi(),
          opts :: opts()
        ) :: {:ok, any()} | {:error, any()} | multi_failure()
  def transaction(fun_or_multi, opts \\ [])

  def transaction(%_{} = multi, opts) do
    opts = Keyword.merge(default_opts(), opts)

    Config.repo!(opts).transaction(multi, opts)
  end

  def transaction(fun, opts) do
    opts = Keyword.merge(default_opts(), opts)

    op = fn repo ->
      result = if is_function(fun, 1), do: fun.(repo), else: fun.()

      if Keyword.get(opts, :rollback_on_error, true) do
        case result do
          :error ->
            repo.rollback(:error)

          {:error, reason} ->
            repo.rollback(reason)

          {:ok, value} ->
            value

          term ->
            term
        end
      else
        result
      end
    end

    case Config.repo!(opts).transaction(op, opts) do
      {:error, :error} -> :error
      {:ok, :ok} -> :ok
      result -> result
    end
  end

  defp create_changeset(query, params, opts) do
    schema_module = CommonSchemas.get_schema_queryable(query)

    if function_exported?(schema_module, :create_changeset, 1) and
         not Keyword.has_key?(opts, :prepare_changeset) do
      schema_module.create_changeset(params)
    else
      CommonSchemas.prepare_changeset(query, params, opts)
    end
  end

  defp maybe_drop_associations(params, query, opts) do
    if Keyword.get(opts, :drop_associations, true) do
      drop_associations(params, query)
    else
      params
    end
  end

  defp drop_associations(params, query) do
    Map.drop(params, CommonSchemas.get_schema_queryable(query).__schema__(:associations))
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

  defp default_opts do
    []
    |> maybe_put_opt(:repo, Config.repo())
    |> maybe_put_opt(:replica, Config.replica())
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, val), do: Keyword.put(opts, key, val)
end
