defmodule EctoShorts.Actions do
  @moduledoc """
  # EctoShorts.Actions

  Provides a standardized API for simplifying Ecto repo operations and
  reducing boilerplate.

  This module offers a declarative, parameter-driven approach to building
  queries and handling data operations. It abstracts away many of the common
  pitfalls of working with Ecto—so you can focus on what your code should do,
  not how to make Ecto do it.

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
    QueryBuilder,
    QueryHelpers
  }

  @doc group: "Schema API"
  @doc """
  ...
  """
  @spec insert_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(),
          opts :: keyword()
        ) :: {non_neg_integer(), nil | [term()]}
  def insert_all(query, params_list, opts) do
    query
    |> CommonParams.convert_to_insert_all_params(params_list, opts)
    |> Config.repo!(opts).insert_all(opts)
  end

  @doc group: "Schema API"
  @doc """
  ...
  """
  @spec update_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map()
        ) :: {non_neg_integer(), nil | [term()]}
  def update_all(query, find_params, update_params) do
    update_all(query, find_params, update_params, default_opts())
  end

  @doc group: "Schema API"
  @doc """
  ...
  """
  @spec update_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map(),
          opts :: keyword()
        ) :: {non_neg_integer(), nil | [term()]}
  def update_all(query, find_params, update_params, opts) do
    query
    |> CommonFilters.convert_params_to_filter(find_params, opts)
    |> Config.repo!(opts).update_all(
      CommonParams.convert_to_update_all_params(query, update_params, opts),
      opts
    )
  end

  @doc group: "Query API"
  @doc """
  ...
  """
  @spec delete_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {non_neg_integer(), nil | [term()]}
  def delete_all(query, params) do
    delete_all(query, params, default_opts())
  end

  @doc group: "Query API"
  @doc """
  ...
  """
  @spec delete_all(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: {non_neg_integer(), nil | [term()]}
  def delete_all(query, params, opts) do
    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end

  @doc group: "Multi API"
  @doc """
  ...

  ### Examples

      iex> SchemasPG.Actions.find_or_create_many(MyApp.User, [%{id: 1, username: "fira", full_name: "Fira"}])
  """
  @spec find_or_create_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map() | {map(), map()})
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_or_create_many(query, params_list) do
    find_or_create_many(query, params_list, default_opts())
  end

  @doc group: "Multi API"
  @doc """
  Retrieves a record matching the given parameters or creates
  a record in sequential order inside a transaction.

  ### Examples

      iex> SchemasPG.Actions.find_or_create_many(MyApp.User, [%{id: 1, username: "fira", full_name: "Fira"}], repo: MyApp.Repo)
      iex> SchemasPG.Actions.find_or_create_many(MyApp.User, [%{id: 1, username: "fira", full_name: "Fira"}], replica: MyApp.Repo.Replica)
      iex> SchemasPG.Actions.find_or_create_many(MyApp.User, [{%{id: 1}, %{username: "fira", full_name: "Fira"}}], repo: MyApp.Repo)

      iex> SchemasPG.Actions.find_or_create_many({"users", MyApp.User}, [%{id: 1, username: "fira", full_name: "Fira"}], repo: MyApp.Repo)
      iex> SchemasPG.Actions.find_or_create_many({"users", MyApp.User}, [%{id: 1, username: "fira", full_name: "Fira"}], replica: MyApp.Repo.Replica)
      iex> SchemasPG.Actions.find_or_create_many({"users", MyApp.User}, [{%{id: 1}, %{username: "fira", full_name: "Fira"}}], repo: MyApp.Repo)
  """
  @spec find_or_create_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map() | {map(), map()}),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_or_create_many(query, params_list, opts) do
    case query
         |> multi_one_or_insert(params_list, opts)
         |> Config.repo!(opts).transaction(opts) do
      {:ok, operations} -> {:ok, Map.values(operations)}
      {:error, _failed_op, _failed_value, _changes_so_far} = e -> e
    end
  end

  defp multi_one_or_insert(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {args, i}, multi ->
      {find_params, create_params} = unzip_find_params(args, query, opts)

      multi
      |> Ecto.Multi.one(
        {:one, i},
        CommonFilters.convert_params_to_filter(query, find_params, opts)
      )
      |> Ecto.Multi.insert({:insert, i}, fn changes_so_far ->
        query
        |> QueryHelpers.get_query_source()
        |> CommonSchemas.prepare_changeset(
          Map.fetch!(changes_so_far, {:one, i}),
          Map.merge(find_params, create_params),
          opts
        )
      end)
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...

  ### Examples

      iex> ...
  """
  @spec find_and_update_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map() | {map(), map()})
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_and_update_many(query, params_list) do
    find_and_update_many(query, params_list, default_opts())
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...

  ### Examples

      iex> ...
  """
  @spec find_and_update_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map() | {map(), map()}),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_and_update_many(query, params_list, opts) do
    case query
         |> multi_one_or_update(params_list, opts)
         |> Config.repo!(opts).transaction(opts) do
      {:ok, operations} -> {:ok, Map.values(operations)}
      {:error, _failed_op, _failed_value, _changes_so_far} = e -> e
    end
  end

  defp multi_one_or_update(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {args, i}, multi ->
      {find_params, update_params} = unzip_find_params(args, query, opts)

      multi
      |> Ecto.Multi.one(
        {:one, i},
        CommonFilters.convert_params_to_filter(query, find_params, opts)
      )
      |> Ecto.Multi.update({:update, i}, fn changes_so_far ->
        query
        |> QueryHelpers.get_query_source()
        |> CommonSchemas.prepare_changeset(
          Map.fetch!(changes_so_far, {:one, i}),
          Map.merge(find_params, update_params),
          opts
        )
      end)
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...

  ### Examples

      iex> ...
  """
  @spec find_and_upsert_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map() | {map(), map()})
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_and_upsert_many(query, params_list) do
    find_and_upsert_many(query, params_list, default_opts())
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...

  ### Examples

      iex> ...
  """
  @spec find_and_upsert_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map() | {map(), map()}),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_and_upsert_many(query, params_list, opts) do
    case query
         |> multi_one_and_insert_or_update(params_list, opts)
         |> Config.repo!(opts).transaction(opts) do
      {:ok, operations} -> {:ok, Map.values(operations)}
      {:error, _failed_op, _failed_value, _changes_so_far} = e -> e
    end
  end

  defp multi_one_and_insert_or_update(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {args, i}, multi ->
      {find_params, upsert_params} = unzip_find_params(args, query, opts)

      multi
      |> Ecto.Multi.one(
        {:one, i},
        CommonFilters.convert_params_to_filter(query, find_params, opts)
      )
      |> Ecto.Multi.insert_or_update({:insert_or_update, i}, fn changes_so_far ->
        query
        |> QueryHelpers.get_query_source()
        |> CommonSchemas.prepare_changeset(
          Map.fetch!(changes_so_far, {:one, i}),
          Map.merge(find_params, upsert_params),
          opts
        )
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
  ...

  ### Examples

      iex> SchemasPG.Actions.create_many(MyApp.User, [%{username: "fira", full_name: "Fira"}])
  """
  @spec create_many(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map())
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def create_many(query, params_list) do
    create_many(query, params_list, default_opts())
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  Creates many records sequentially in a transaction.

  ### Examples

      iex> SchemasPG.Actions.create_many(MyApp.User, [%{username: "fira", full_name: "Fira"}], repo: MyApp.Repo)
  """
  @spec create_many(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map()),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def create_many(query, params_list, opts) do
    case query
         |> multi_insert(params_list, opts)
         |> Config.repo!(opts).transaction(opts) do
      {:ok, operations} -> {:ok, Map.values(operations)}
      {:error, _failed_op, _failed_value, _changes_so_far} = e -> e
    end
  end

  defp multi_insert(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, i}, multi ->
      Ecto.Multi.insert(multi, i, CommonSchemas.prepare_changeset(query, params, opts))
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...

  ### Examples

      iex> SchemasPG.Actions.find_many(MyApp.User, [%{username: "fira"}])
  """
  @spec find_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map())
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_many(query, params_list) do
    find_many(query, params_list, default_opts())
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  Retrieves many records matching the parameters sequentially in a transaction.

  ### Examples

      iex> SchemasPG.Actions.find_many(MyApp.User, [%{username: "fira"}], repo: MyApp.Repo.Replica)
      iex> SchemasPG.Actions.find_many(MyApp.User, [%{username: "fira"}], replica: MyApp.Repo.Replica)

      iex> SchemasPG.Actions.find_many({"users", MyApp.User}, [%{username: "fira"}], repo: MyApp.Repo.Replica)
      iex> SchemasPG.Actions.find_many({"users", MyApp.User}, [%{username: "fira"}], replica: MyApp.Repo.Replica)
  """
  @spec find_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map()),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def find_many(query, params_list, opts) do
    case query |> multi_one(params_list, opts) |> Config.repo!(opts).transaction(opts) do
      {:ok, operations} ->
        {:ok, Map.values(operations)}

      {:error, _failed_op, _failed_value, _changes_so_far} = e ->
        e
    end
  end

  defp multi_one(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, i}, multi ->
      Ecto.Multi.one(multi, i, CommonFilters.convert_params_to_filter(query, params, opts))
    end)
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  ...
  """
  @spec delete_many(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params_list :: list(map()),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | Ecto.Multi.failure()
  def delete_many(query, params_list, opts) do
    case query |> multi_delete(params_list, opts) |> Config.repo!(opts).transaction(opts) do
      {:ok, operations} ->
        {:ok, Map.values(operations)}

      {:error, _failed_op, _failed_value, _changes_so_far} = e ->
        e
    end
  end

  defp multi_delete(query, params_list, opts) do
    params_list
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {params, i}, multi ->
      Ecto.Multi.delete(multi, i, CommonFilters.convert_params_to_filter(query, params, opts))
    end)
  end

  @doc """
  Equivalent to `find_or_create(query, params, [])`.

  ## Options

  See `find_or_create/3` for options.

  ## Examples

      iex> EctoShorts.Actions.find_or_create(MyApp.User, %{name: "Fira"})
  """
  @spec find_or_create(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_or_create(query, params) do
    find_or_create(query, params, default_opts())
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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_or_create(query, params, opts) do
    with {:error, %{code: :not_found}} <-
           find(query, maybe_drop_associations(params, query, opts), opts) do
      query
      |> QueryHelpers.get_query_source()
      |> create(params, opts)
    end
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

    iex> EctoShorts.Actions.find_or_create(MyApp.User, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo)
    iex> EctoShorts.Actions.find_or_create(MyApp.User, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, replica: MyApp.Repo.Replica)
    iex> EctoShorts.Actions.find_or_create(MyApp.User, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo, replica: MyApp.Repo.Replica)

    iex> EctoShorts.Actions.find_or_create({"users", MyApp.User}, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo)
    iex> EctoShorts.Actions.find_or_create({"users", MyApp.User}, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, replica: MyApp.Repo.Replica)
    iex> EctoShorts.Actions.find_or_create({"users", MyApp.User}, %{email: "fira@example.com"}, %{email: "fira@example.com", name: "Fira"}, repo: MyApp.Repo, replica: MyApp.Repo.Replica)
  """
  @spec find_or_create(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          create_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_or_create(query, find_params, create_params, opts) do
    with {:error, %{code: :not_found}} <- find(query, find_params, opts) do
      query
      |> QueryHelpers.get_query_source()
      |> create(create_params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.find_and_update(MyApp.User, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_update({"users", MyApp.User}, %{id: 1}, %{name: "Fira"})
  """
  @spec find_and_update(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_and_update(query, find_params, update_params) do
    find_and_update(query, find_params, update_params, default_opts())
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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_and_update(query, find_params, update_params, opts) do
    with {:ok, struct} <- find(query, find_params, opts) do
      query
      |> QueryHelpers.get_query_source()
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
      iex> EctoShorts.Actions.find_and_upsert({"users", MyApp.User}, %{id: 1}, %{name: "Fira"})
  """
  @spec find_and_upsert(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_and_upsert(query, find_params, update_params) do
    find_and_upsert(query, find_params, update_params, default_opts())
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

      iex> EctoShorts.Actions.find_and_upsert(MyApp.User, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_upsert(MyApp.User, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_and_upsert({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_upsert({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_and_upsert(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          update_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_and_upsert(query, find_params, update_params, opts) do
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

  ## Examples

      iex> EctoShorts.Actions.find_and_delete(MyApp.User, %{id: 1}, %{name: "Fira"})
      iex> EctoShorts.Actions.find_and_delete({"users", MyApp.User}, %{id: 1}, %{name: "Fira"})
  """
  @spec find_and_delete(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_and_delete(query, find_params) do
    find_and_delete(query, find_params, default_opts())
  end

  @doc group: "Query API"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.find_and_delete(MyApp.User, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_delete(MyApp.User, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)

      iex> EctoShorts.Actions.find_and_delete({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, repo: MyApp.Repo)
      iex> EctoShorts.Actions.find_and_delete({"users", MyApp.User}, %{id: 1}, %{name: "Fira"}, replica: MyApp.Repo.Replica)
  """
  @spec find_and_delete(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          find_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def find_and_delete(query, find_params, opts) do
    with {:ok, struct} <- find(query, find_params, opts) do
      delete(struct, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.get(MyApp.User, 1)
      iex> EctoShorts.Actions.get({"users", MyApp.User}, 1)
  """
  @spec get(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id :: integer() | binary()
        ) :: Ecto.Schema.t() | nil
  def get(query, id) do
    get(query, id, default_opts())
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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id :: integer() | binary(),
          opts :: keyword()
        ) :: Ecto.Schema.t() | nil
  def get(query, id, opts) do
    Config.replica!(opts).get(query, id, opts)
  end

  @doc group: "Query API"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.all(MyApp.User)
      iex> EctoShorts.Actions.all({"users", MyApp.User})
  """
  @spec all(query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()}) ::
          list(Ecto.Schema.t())
  def all(query) do
    all(query, %{})
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

      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1})
      iex> EctoShorts.Actions.all(MyApp.User, id: 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all(MyApp.User, id: 1, replica: MyApp.Repo)

      iex> EctoShorts.Actions.all({"users", MyApp.User}, %{id: 1})
      iex> EctoShorts.Actions.all({"users", MyApp.User}, id: 1, repo: MyApp.Repo)
      iex> EctoShorts.Actions.all({"users", MyApp.User}, id: 1, replica: MyApp.Repo)
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
    all(query, params, default_opts())
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
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).all(opts)
  end

  @doc group: "Schema API"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.create(MyApp.User, %{name: "Fira"})
      iex> EctoShorts.Actions.create({"users", MyApp.User}, %{name: "Fira"})
  """
  @spec create(
          query :: Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def create(query, params) do
    create(query, params, default_opts())
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
          params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def create(query, params, opts) do
    query
    |> CommonSchemas.prepare_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "Query API"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.find(MyApp.User, %{id: 1})
      iex> EctoShorts.Actions.find({"users", MyApp.User}, %{id: 1})
  """
  @spec find(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, any()}
  def find(query, params) do
    find(query, params, default_opts())
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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, any()}
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
    IO.inspect(binding(), label: "find")

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
  ...

  ## Examples

      iex> EctoShorts.Actions.update(MyApp.User, 1, %{username: "fira"})
      iex> EctoShorts.Actions.update(MyApp.User, %MyApp.User{}, %{username: "fira"})

      iex> EctoShorts.Actions.update({"users", MyApp.User}, 1, %{username: "fira"})
      iex> EctoShorts.Actions.update({"users", MyApp.User}, %MyApp.User{}, %{username: "fira"})
  """
  @spec update(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id_or_struct :: integer() | binary() | Ecto.Schema.t(),
          update_params :: map()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def update(query, id_or_struct, update_params) do
    update(query, id_or_struct, update_params, default_opts())
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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id_or_struct :: integer() | binary() | Ecto.Schema.t(),
          update_params :: map(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def update(query, id, update_params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, struct} <- find(query, %{id: id}, opts) do
      query
      |> QueryHelpers.get_query_source()
      |> update(struct, update_params, opts)
    end
  end

  def update(query, struct, update_params, opts) when is_list(update_params) do
    update(query, struct, Map.new(update_params), opts)
  end

  def update(query, struct, update_params, opts) do
    query
    |> CommonSchemas.prepare_changeset(struct, update_params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc """
  Deletes a record given existing data.

  Note: When a list of parameters are specified they are deleted in order
  and are not wrapped in a transaction.

  ## Options

  This uses the default options defined in your configuration.

  ## Examples

      iex> EctoShorts.Actions.delete(%MyApp.User{})
      iex> EctoShorts.Actions.delete([%MyApp.User{}])
  """
  @spec delete(struct :: Ecto.Schema.t()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  @spec delete(structs_or_changesets :: list(Ecto.Schema.t())) ::
          {:ok, list(Ecto.Schema.t())}
          | {:error, list(Ecto.Changeset.t())}
          | {:error, list(any())}
  def delete(%_{} = struct) do
    delete(struct, default_opts())
  end

  def delete(structs_or_changesets) when is_list(structs_or_changesets) do
    delete(structs_or_changesets, default_opts())
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
          structs_or_changesets :: list(Ecto.Schema.t()) | list(Ecto.Changeset.t()),
          opts :: keyword()
        ) :: {:ok, list(Ecto.Schema.t())} | {:error, list(Ecto.Changeset.t())} | {:error, any()}
  @spec delete(
          struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def delete(%_{data: %_{__meta__: %{schema: queryable}}} = changeset, opts) do
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

  def delete(structs_or_changesets, opts) when is_list(structs_or_changesets) do
    EctoShorts.Utils.reduce_all(structs_or_changesets, fn struct_or_changeset ->
      delete(struct_or_changeset, opts)
    end)
  end

  def delete(query, id) when is_binary(id) or is_integer(id) do
    delete(query, id, default_opts())
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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          id :: integer() | binary(),
          opts :: keyword()
        ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, any()}
  def delete(query, id, opts) when is_integer(id) or is_binary(id) do
    with {:ok, struct} <- find(query, %{id: id}, opts) do
      delete(struct, opts)
    end
  end

  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.stream(MyApp.User, %{id: 1})
      iex> EctoShorts.Actions.stream({"users", MyApp.User}, %{id: 1})
  """
  @spec stream(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map()
        ) :: list(any())
  def stream(query, params) do
    stream(query, params, default_opts())
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
  @spec stream(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          opts :: keyword()
        ) :: list(any())
  def stream(query, params, opts) do
    query
    |> QueryBuilder.Expression.from(opts[:from] || %{})
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.aggregate(MyApp.User, %{id: 1}, :count, :id)

      iex> EctoShorts.Actions.aggregate({"users", MyApp.User}, %{id: 1}, :count, :id)
  """
  @spec aggregate(
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          aggregate :: :avg | :count | :max | :min | :sum,
          field :: atom()
        ) :: {:ok, any()} | {:error, any()}
  def aggregate(query, params, aggregate, field) do
    aggregate(query, params, aggregate, field, default_opts())
  end

  @doc """
  Calculates an aggregate value for `field`, using the specified `aggregate` function.

  This function builds a query from the given schema or queryable, applies filters from `params`,
  and calculates the aggregate (e.g. `:count`, `:sum`, etc.) on the specified field.

  ## Supported Aggregates

  The following aggregate types are supported (same as `Ecto.Repo.aggregate/4`):

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
          query :: Ecto.Query.t() | Ecto.Queryable.t() | {binary(), Ecto.Queryable.t()},
          params :: map(),
          aggregate :: :avg | :count | :max | :min | :sum,
          field :: atom(),
          opts :: keyword()
        ) :: {:ok, any()} | {:error, any()}
  def aggregate(query, params, aggregate, field, opts) do
    query
    |> QueryBuilder.Expression.from(opts[:from] || %{})
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, field, opts)
  end

  @doc group: "Transaction API"
  @doc since: "2.5.0"
  @doc """
  ...

  ## Examples

      iex> EctoShorts.Actions.transaction(MyApp.User, fn -> :ok end)
      iex> EctoShorts.Actions.transaction({"users", MyApp.User}, fn -> :ok end)
  """
  def transaction(fun_or_multi) do
    transaction(fun_or_multi, default_opts())
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
  @spec transaction(
          fun_or_multi :: function() | Ecto.Multi.t(),
          opts :: keyword()
        ) :: {:ok, any()} | {:error, any()} | Ecto.Multi.failure()
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
            The function executed within the transaction returned an unexpected result.

            Expected one of:

              - :ok
              - :error
              - {:ok, any()}
              - {:error, any()}

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
