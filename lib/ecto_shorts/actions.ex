defmodule EctoShorts.Actions do
  @moduledoc """
  A simplified interface for working with your Ecto schemas using a
  map-based, data-driven approach.

  `EctoShorts.Actions` lets you query, insert, update, and delete records
  using parameters instead of the traditional Ecto query and changeset APIs.
  You describe what you want to do, and the API takes care of building the
  necessary query expressions or changesets behind the scenes.

  This leads to cleaner, more maintainable code in your codebase.

  Instead of manually writing queries like this:

      User |> where([u], u.name == "Jane") |> Repo.all()

  You can write:

      EctoShorts.Actions.all(User, %{name: "Jane"})

  Similarly, data inserts and updates are simplified:

      EctoShorts.Actions.create(Post, %{title: "Hello", body: "World"})
      EctoShorts.Actions.update(Post, 1, %{title: "Updated Title"})

  Each function in this module works with maps. You define what you
  want using simple parameters, and the module handles building
  the query, resolving keys, or transforming data accordingly.

  ## Filter Parameters

  The core idea behind the API is to let you describe what you're looking
  for using data. Each map expresses a set of conditions, and the API
  translates those conditions into the appropriate Ecto query expressions
  for you.

  This means you don't need to write query logic directly. Instead, you
  focus on describing your intent, and EctoShorts handles the transformation.

  For example:

      %{name: "Jane"}
      # Matches records where `name` equals "Jane"

      %{title: %{ilike: "post"}}
      # Case-insensitive match on the `title` field

      %{comments: %{body: %{ilike: "example"}}}
      # Joins the `comments` association and applies a case-insensitive filter on the body

      %{inserted_at: %{gt: ~N[2023-01-01 00:00:00]}}
      # Matches records inserted after January 1, 2023

  You can compose deeply nested filters, use operators like `:gt`, `:lt`, `:in`,
  and filter on associated tables without needing to write `join` or `where`
  clauses manually.

  This approach makes your application logic simpler to write and easier to
  maintain, especially when filters are built dynamically from user input or
  external sources.

  ## Batching

  Batching lets you fetch many records at once using a list of maps. Each
  map represents a distinct set of criteria. Instead of running separate
  queries for each item, the API builds a single query using OR conditions
  to return everything in one operation.

  You describe what you want to find, and EctoShorts builds a single Ecto
  query to fetch all results.

  For example:

      params_list = [
        %{author_id: 1, status: "published"},
        %{author_id: 2, status: "draft"}
      ]

      EctoShorts.Actions.batch_find(Post, params_list)

  Each map describes its own match condition. The function returns all
  records that match at least one of them.

  Batching is especially helpful when resolving fields in GraphQL APIs,
  loading related data in bulk, or handling incoming parameters that
  need to be mapped to existing records efficiently.

  ## Multi API

  Multi operations let you group together several changes like inserts,
  updates, or deletes — and run them inside a single database transaction.

  Each action is executed sequentially in the order provided. If any step
  fails (for example, due to a validation error), the entire set of changes
  is rolled back automatically.

  This makes the Multi API especially useful when you need to apply a group
  of related changes where all or nothing should happen.

  For example:

      multi = EctoShorts.Actions.multi_create(%{
        post: {MyApp.Post, %{title: "Hello"}},
        log: {MyApp.Log, %{message: "created post"}}
      })

      Repo.transaction(multi)

  Each entry is processed in order, and all changes are wrapped in a transaction.
  You stay focused on describing what changes need to happen, and the underlying
  logic is handled for you.

  ## Shared Options

  Most functions support these shared options:

    - `:repo` — Specifies the Ecto repo module to use. The options passed
      at runtime are checked first and then the application environment.
      If neither is present, an error is raised.

    - `:replica` — Routes read operations to a read replica. This is useful
      if your application uses replicas to offload traffic from the primary
      database.
  """

  alias EctoShorts.{
    Actions.Error,
    CommonFilters,
    CommonParams,
    CommonSchema,
    Config,
    CommonQuery,
    Utils
  }

  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type sourceable :: schema_module() | {schema_source(), schema_module()}
  @type changeset :: Ecto.Changeset.t()
  @type schema_data :: Ecto.Schema.t()

  @type multi :: Ecto.Multi.t()
  @type multi_failure :: Ecto.Multi.failure()
  @type multi_params :: list(map() | {map(), map()})

  @type batch_key :: atom() | list(atom())
  @type batch_id :: map()
  @type batch_params :: params() | {params(), params()} | {schema_data(), params()}

  @type stream :: Enumerable.t()
  @type id :: integer() | binary()
  @type key :: atom()
  @type params :: map()
  @type opts :: keyword()

  @type aggregate_options :: :avg | :count | :max | :min | :sum
  @type preloads :: list() | keyword()

  @doc since: "2.5.0"
  @doc """
  Preloads associations onto a struct or list of structs.

  ## Options

  This function supports the `:repo` option, which is described in the
  [Shared Options](#module-shared-options) section of the module documentation.

  ## Examples

      # Preload a single association on one record
      iex> user = EctoShorts.Actions.all(User, %{name: "Jane"}) |> List.first()
      iex> user = EctoShorts.Actions.preload(user, [:posts])
      iex> user.posts
      [%EctoShorts.Schema.Post{}, %EctoShorts.Schema.Post{}]

      # Preload multiple associations on a list of records
      iex> users = EctoShorts.Actions.all(User, %{status: "active"})
      iex> users = EctoShorts.Actions.preload(users, [:posts, :profile])
      iex> Enum.all?(users, &Map.has_key?(&1, :profile))
      true
  """
  @spec preload(schema_data() | list(schema_data()), preloads(), opts()) ::
          schema_data() | list(schema_data())
  def preload(schema_data, preloads, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    Config.replica!(opts).preload(schema_data, preloads, opts)
  end

  @doc group: "Batch API"
  @doc since: "2.5.0"
  @doc """
  Loads records that match a list of input parameters and returns
  them alongside each input.

  This function is useful when you have a list of values typically
  maps or `{input, context}` tuples and want to fetch all records
  that match them using a single database query.

  Each input is then returned along with its corresponding loaded
  record, allowing you to process the data in the same structure
  it was received.

  ## Options

  This function supports the `:repo` option, which is described in the
  [Shared Options](#module-shared-options) section of the module documentation.

  ## Examples

      # Find posts for a list of lookup maps
      iex> values = [%{author_id: 1}, %{author_id: 2}]
      iex> result = EctoShorts.Actions.batch_load(Post, values, :author_id)
      iex> Enum.all?(result, fn {_, post} -> post.author_id in [1, 2] end)
      true

      # Load users and preserve the original input
      iex> params = [%{name: "A"}, %{name: "B"}]
      iex> result = EctoShorts.Actions.batch_load(User, params, :name)
      iex> Enum.map(result, fn {input, user} -> {input[:name], user.name} end)
      [
        {"A", "A"},
        {"B", "B"}
      ]
  """
  @spec batch_load(
          query() | sourceable(),
          batch_key() | :primary_key,
          list(batch_params())
        ) :: list(batch_params())
  @spec batch_load(
          query() | sourceable(),
          batch_key() | :primary_key,
          list(batch_params()),
          opts()
        ) :: list(batch_params())
  def batch_load(query, batch_key \\ :primary_key, params_list, opts \\ []) do
    batch_key = normalize_batch_key(query, batch_key)

    case filter_batch_params(params_list, batch_key) do
      [] ->
        params_list

      batch_params ->
        with batch_results <- batch(query, batch_key, batch_params, opts) do
          merge_batch_results(params_list, batch_results, batch_key, query)
        end
    end
  end

  defp merge_batch_results(params_list, batch_results, batch_key, query) do
    Enum.map(params_list, fn
      {find_params, params} when is_map(find_params) and not is_struct(find_params) ->
        if all_keys?(batch_key, find_params) do
          batch_id = batch_id!(query, batch_key, find_params)

          case Map.get(batch_results, batch_id) do
            nil -> {find_params, params}
            schema_data -> {schema_data, params}
          end
        else
          params
        end

      params when is_map(params) and not is_struct(params) ->
        if all_keys?(batch_key, params) do
          batch_id = batch_id!(query, batch_key, params)

          case Map.get(batch_results, batch_id) do
            nil -> params
            schema_data -> {schema_data, params}
          end
        else
          params
        end

      value ->
        value
    end)
  end

  defp filter_batch_params(params_list, batch_key) do
    Enum.reduce(params_list, [], fn
      {params, _}, acc when is_map(params) and not is_struct(params) ->
        if all_keys?(batch_key, params) do
          [params | acc]
        else
          acc
        end

      params, acc when is_map(params) and not is_struct(params) ->
        if all_keys?(batch_key, params) do
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
  Finds all records that match any of the given filter parameters.

  Each map in the list is treated as an individual filter using an OR condition,
  and the query returns all records that match at least one of them.

  ## Options

  This function supports the `:repo` and `:replica` options, which are described in the
  [Shared Options](#module-shared-options) section of the module documentation.

  ## Examples

      # Find all users matching any of the filter criteria
      iex> filters = [
      ...>   %{email: "user1@example.com"},
      ...>   %{name: "User 2"},
      ...>   %{org_id: 3}
      ...> ]
      iex> EctoShorts.Actions.batch_find(User, filters)
      [%User{}, %User{}, ...]
  """
  @spec batch_find(
          query() | sourceable(),
          batch_key() | :primary_key,
          list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec batch_find(
          query() | sourceable(),
          batch_key() | :primary_key,
          list(params()),
          opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def batch_find(query, batch_key \\ :primary_key, params_list, opts \\ []) do
    batch_key = normalize_batch_key(query, batch_key)

    batch_results = batch(query, batch_key, params_list, opts)

    params_list
    |> Stream.with_index()
    |> Utils.reduce_all(fn {params, i} ->
      batch_id = batch_id!(query, batch_key, params)

      case Map.get(batch_results, batch_id) do
        nil ->
          {:error,
           Error.call(:not_found, "Record not found.", %{
             query: query,
             params: params_list,
             failed_value: params,
             position: i,
             batch_key: batch_key
           })}

        schema_data ->
          {:ok, schema_data}
      end
    end)
  end

  @doc group: "Batch API"
  @doc """
  Performs a batched query and returns a map of results keyed by input parameters.

  Batching is a common technique used to reduce redundant queries by grouping
  similar fetch operations into a single database query. This is especially useful
  when handling many records at once, such as resolving fields in a GraphQL API
  or processing a list of identifiers in bulk.

  Instead of issuing one query per parameter set, this function combines all
  parameter sets into a single query and returns a result map that preserves
  the association between the original inputs and the fetched records.

      iex> EctoShorts.Actions.batch(Post, [:title], [
      ...>   %{title: "First Post"},
      ...>   %{title: "Second Post"}
      ...> ])

  Returns:

      %{
        %{title: "First Post"} => %EctoShorts.Schema.Post{title: "First Post"},
        %{title: "Second Post"} => %EctoShorts.Schema.Post{title: "Second Post"}
      }

  ## Composite keys

  You can specify a list of keys to match on using the `keys` argument:

      iex> EctoShorts.Actions.batch(User, [:org_id, :email], [
      ...>   %{org_id: 1, email: "admin@example.com"},
      ...>   %{org_id: 2, email: "editor@example.com"}
      ...> ])

  This ensures each input map is matched against all specified fields as a unique
  composite key.

  If no `keys` is provided, the schema's primary key is used automatically.
  If your schema does not define a primary key and no batch key is given,
  this function will raise an error.

  ## Examples

      iex> Actions.batch(Post, [:title], [%{title: "post_title"}])
      %{%{title: "post_title"} => %EctoShorts.Schema.Post{title: "post_title"}}
  """
  @spec batch(
          query() | sourceable(),
          batch_key() | :primary_key,
          list(params())
        ) :: %{batch_id() => schema_data()}
  @spec batch(
          query() | sourceable(),
          batch_key() | :primary_key,
          list(params()),
          opts()
        ) :: %{batch_id() => schema_data()}
  def batch(query, batch_key \\ :primary_key, params_list, opts \\ []) do
    batch_key = normalize_batch_key(query, batch_key)

    query
    |> all(%{or_where: params_list}, opts)
    |> Enum.group_by(fn schema_data -> batch_id!(query, batch_key, schema_data) end)
    |> Enum.filter(fn
      {_batch_id, []} -> false
      {_batch_id, [_schema_data]} -> true
      {_batch_id, records} -> raise "Expected one record for, got #{length(records)}"
    end)
    |> Map.new(fn {batch_id, [schema_data]} -> {batch_id, schema_data} end)
  end

  defp batch_id!(query, batch_key, data) when is_list(batch_key) do
    schema_module = CommonSchema.module_for_schema(query)

    if all_keys?(batch_key, data) do
      Map.take(data, batch_key)
    else
      raise KeyError, """
      Batch key not found for schema #{inspect(schema_module)}.

      batch key:
      #{inspect(batch_key)}

      data:
      #{inspect(data, pretty: true)}
      """
    end
  end

  defp normalize_batch_key(query, :primary_key) do
    with [] <- CommonSchema.reflection_for_schema(query, :primary_key) do
      [:id]
    end
  end

  defp normalize_batch_key(_query, value) do
    with [] <- List.wrap(value) do
      [:id]
    end
  end

  defp all_keys?([], _data) do
    false
  end

  defp all_keys?(keys, data) do
    Enum.all?(keys, &(Map.get(data, &1) !== nil))
  end

  @doc group: "Schema API"
  @doc since: "2.5.0"
  @doc """
  Inserts multiple records into the database.

  Each entry in the `params_list` is validated using the schema's `changeset/2` function
  unless `validate: false` is passed.

  ## Conflict Handling

  If a record includes all primary key fields, it will be treated as an upsert.
  It automatically applies:

    * `:conflict_target` set to the primary key
    * `:on_conflict` set to `{:replace, keys}` — where `keys` are all fields except the
      primary key and `:inserted_at`

  This allows partial updates on existing records during bulk inserts while skipping
  fields that cannot be nil.

  ## Preloading Existing Records

  Sometimes, you may need to preload existing records so that the changeset has access
  to required values:

    * If your changeset logic uses data from the existing row (e.g., to preserve an audit field).
    * If a required field cannot be overwritten by `nil`, and passing it directly would cause
      a constraint error.

  You can enable this behavior with the `:batch_load` option:

      Actions.insert_all(User, params_list, batch_load: :primary_key)

  This will call `batch_load/4` and transform each param into
  `{existing_record, new_params}` tuples when a match is found.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  You can also pass any options accepted by [`Ecto.Repo.insert_all/3`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert_all/3).

  ## Examples

      iex> EctoShorts.Actions.insert_all(MyApp.User, [%{id: 1, name: "Jane"}], batch_load: :primary_key)
      {:ok, {1, [%User{id: 1, name: "Jane"}]}}
  """
  @spec insert_all(
          query() | sourceable(),
          list(params())
        ) :: {:ok, {non_neg_integer(), nil | [term()]}} | {:error, any()}
  @spec insert_all(
          query() | sourceable(),
          list(params()),
          opts()
        ) :: {:ok, {non_neg_integer(), nil | [term()]}} | {:error, any()}
  def insert_all(query, params_list, opts \\ []) do
    schema_module = CommonSchema.module_for_schema(query)

    with {:ok, inserts, insert_opts} <-
           CommonParams.convert_to_insert_all_params(
             schema_module,
             maybe_batch_load(query, params_list, opts),
             opts
           ) do
      {:ok,
       Config.repo!(opts).insert_all(
         schema_module,
         inserts,
         Keyword.merge(insert_opts, opts)
       )}
    end
  end

  defp maybe_batch_load(query, params_list, opts) do
    cond do
      opts[:batch_load] === true ->
        batch_load(query, :primary_key, params_list, opts)

      Keyword.has_key?(opts, :batch_key) ->
        batch_load(query, opts[:batch_key], params_list, opts)

      true ->
        params_list
    end
  end

  @doc group: "Schema API"
  @doc """
  Updates multiple records that match the given filter parameters.

  This function builds a query from the given schema or queryable, applies
  filters using the `find_params`, and performs a bulk update with the
  provided `update_params`.

  Unlike `update/4`, which works on a single struct or ID, this function
  updates all matching rows in one operation. This is useful when applying
  the same update across many records.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  You can also pass any options accepted by [`Ecto.Repo.update_all/3`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:update_all/3).

  ## Examples

      iex> EctoShorts.Actions.update_all(MyApp.User, %{active: false}, %{active: true})
      {5, nil}

      iex> EctoShorts.Actions.update_all(MyApp.User, %{role: "guest"}, %{role: "user"}, repo: MyApp.Repo)
      {12, nil}
  """
  @spec update_all(
          query() | sourceable(),
          params(),
          params()
        ) :: {non_neg_integer(), nil | [term()]}
  @spec update_all(
          query() | sourceable(),
          params(),
          params(),
          opts()
        ) :: {non_neg_integer(), nil | [term()]}
  def update_all(query, find_params, update_params, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    updates =
      query
      |> CommonSchema.module_for_schema()
      |> CommonParams.convert_to_update_all_params(update_params, opts)

    query
    |> CommonFilters.convert_params_to_filter(find_params, opts)
    |> Config.repo!(opts).update_all(updates, opts)
  end

  @doc group: "Query API"
  @doc """
  Deletes all records that match the given filter parameters.

  This function builds a query from the provided schema or queryable,
  applies filters using the `params`, and removes all matching records
  from the database in a single operation.

  Unlike `delete/2`, which deletes one record at a time (by ID or struct),
  `delete_all/3` is more efficient for bulk deletions and does not call
  changesets or run validations.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  You can also pass any options accepted by
  [`Ecto.Repo.delete_all/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete_all/2).

  ## Examples

      iex> EctoShorts.Actions.delete_all(MyApp.User, %{active: false})
      {3, nil}

      iex> EctoShorts.Actions.delete_all(MyApp.Post, %{archived: true}, repo: MyApp.Repo)
      {7, nil}
  """
  @spec delete_all(
          query() | sourceable(),
          params()
        ) :: {non_neg_integer(), nil | [term()]}
  @spec delete_all(
          query() | sourceable(),
          params(),
          opts()
        ) :: {non_neg_integer(), nil | [term()]}
  def delete_all(query, params, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end

  @doc group: "Multi API"
  @doc """
  Finds or creates many records sequentially inside a transaction.

  This function supports distinct parameters per operation. You can either:

    * Pass a single map to use as both the `find_params` and `create_params`, or
    * Pass a `{find_params, create_params}` tuple to look up a record with one set
      of values and create it with another if not found.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

    iex> params_list = [
    ...>   %{email: "user1@example.com", name: "User 1"},
    ...>   {%{email: "user2@example.com"}, %{email: "user2@example.com", name: "User 2"}}
    ...> ]
    ...> EctoShorts.Actions.find_or_create_many(MyApp.User, params_list)
    {:ok, [%User{}, %User{}]}
  """
  @spec find_or_create_many(
          query() | sourceable(),
          list(multi_params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_or_create_many(
          query() | sourceable(),
          list(multi_params()),
          opts()
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
                  failed_value: params
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
  Finds and updates many records sequentially inside a transaction.

  Each item in the input list must be either:

    * A map – used as both the `find_params` and `update_params`, or
    * A tuple `{find_params, update_params}` – allows using separate values
      for looking up and updating a record.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> updates = [
      ...>   %{id: 1, name: "Updated Name"},
      ...>   {%{id: 2}, %{name: "Another Update"}}
      ...> ]
      ...> EctoShorts.Actions.find_and_update_many(MyApp.User, updates)
      {:ok, [%User{}, %User{}]}
  """
  @spec find_and_update_many(
          query() | sourceable(),
          list(multi_params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_and_update_many(
          query() | sourceable(),
          list(multi_params()),
          opts()
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
                   |> CommonSchema.build_changeset(
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
  Finds and upserts many records sequentially inside a transaction.

  Each item must be either:

    * A map – used as both the `find_params` and `upsert_params`, or
    * A tuple `{find_params, upsert_params}` – used to separate the values
      for lookup and update/insert.

  If any step fails (e.g., validation errors or unexpected conflicts), the entire
  transaction is rolled back.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> items = [
      ...>   %{email: "user1@example.com", name: "User One"},
      ...>   {%{email: "user2@example.com"}, %{name: "User Two"}}
      ...> ]
      ...> EctoShorts.Actions.find_and_upsert_many(MyApp.User, items)
      {:ok, [%User{}, %User{}]}
  """
  @spec find_and_upsert_many(
          query() | sourceable(),
          list(multi_params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_and_upsert_many(
          query() | sourceable(),
          list(multi_params()),
          opts()
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
                   |> CommonSchema.build_changeset(
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
    {maybe_filter_queryable_params(params, query, opts), params}
  end

  @doc group: "Multi API"
  @doc since: "2.5.0"
  @doc """
  Creates many records sequentially inside a transaction.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> users = [
      ...>   %{email: "user1@example.com", name: "User One"},
      ...>   %{email: "user2@example.com", name: "User Two"}
      ...> ]
      ...> EctoShorts.Actions.create_many(MyApp.User, users)
      {:ok, [%User{}, %User{}]}
  """
  @spec create_many(
          sourceable(),
          list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec create_many(
          sourceable(),
          list(params()),
          opts()
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
               |> CommonSchema.build_changeset(params, opts)
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
  Finds many records sequentially inside a transaction.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> params_list = [
      ...>   %{email: "user1@example.com"},
      ...>   %{email: "user2@example.com"}
      ...> ]
      ...> EctoShorts.Actions.find_many(MyApp.User, params_list)
      {:ok, [%User{}, %User{}]}
  """
  @spec find_many(
          query() | sourceable(),
          list(params())
        ) :: {:ok, list(schema_data())} | {:error, any()}
  @spec find_many(
          query() | sourceable(),
          list(params()),
          opts()
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
  Deletes many records sequentially inside a transaction given a
  list of structs or changesets.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> users = [%User{id: 1}, %User{id: 2}]
      ...> EctoShorts.Actions.delete_many(users)
      {:ok, [%User{}, %User{}]}
  """
  @spec delete_many(list(schema_data() | changeset())) ::
          {:ok, list(schema_data())} | {:error, any()}
  @spec delete_many(
          list(schema_data() | changeset()),
          opts()
        ) :: {:ok, list(schema_data())} | {:error, any()}
  def delete_many(entries, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    entries
    |> multi_delete(opts)
    |> Config.repo!(opts).transaction(opts)
    |> handle_multi_response()
  end

  defp multi_delete(entries, opts) do
    entries
    |> Enum.with_index()
    |> Enum.reduce(Ecto.Multi.new(), fn {schema_data_or_changeset, i}, multi ->
      Ecto.Multi.run(multi, {:create, i}, fn repo, _changes_so_far ->
        with {:error, changeset} <-
               schema_data_or_changeset
               |> CommonSchema.build_changeset(%{}, opts)
               |> repo.delete(opts) do
          {:error,
           {:conflict, "Failed to delete record.",
            %{
              query: CommonSchema.metadata_for_schema(schema_data_or_changeset).schema,
              position: 1,
              changeset: changeset,
              params: entries
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

  @doc group: "Query API"
  @doc """
  Finds a record using one set of parameters, and creates it using
  another if not found.

  This function allows you to use different values for searching and
  inserting. It first attempts to find a record using `find_params`.
  If no match is found, it merges `find_params` with `create_params`
  and inserts a new record.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> find_params = %{email: "user@example.com"}
      ...> create_params = %{email: "user@example.com", name: "New User"}
      ...> EctoShorts.Actions.find_and_create(MyApp.User, find_params, create_params)
      {:ok, %User{}}
  """
  @spec find_and_create(query() | sourceable(), params(), params()) ::
          {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_create(
          query() | sourceable(),
          params(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_create(query, find_params, create_params, opts \\ []) do
    with {:error, %{code: :not_found}} <- find(query, find_params, opts) do
      query
      |> CommonQuery.source_for_query()
      |> create(create_params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Finds a record using `find_params` and updates it with the combined
  data from both `find_params` and `update_params`.

  If the record is not found, an error is returned.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> EctoShorts.Actions.find_and_update(MyApp.User, %{id: 1}, %{name: "Updated Name"})
      {:ok, %User{name: "Updated Name"}}
  """
  @spec find_and_update(
          query() | sourceable(),
          params(),
          params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_update(
          query() | sourceable(),
          params(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_update(query, find_params, update_params, opts \\ []) do
    with {:ok, schema_data} <- find(query, find_params, opts) do
      query
      |> CommonQuery.source_for_query()
      |> update(schema_data, update_params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Finds a record using `find_params` and updates it with `update_params`.
  If the record is not found, a new one is created using the combined
  params.

  If an update fails (e.g. due to validation errors), an error is
  returned. If a create fails, an error is also returned.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> EctoShorts.Actions.find_and_upsert(MyApp.User, %{email: "fira@example.com"}, %{name: "Fira"})
      {:ok, %User{}}
  """
  @spec find_and_upsert(
          query() | sourceable(),
          params(),
          params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_upsert(
          query() | sourceable(),
          params(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_upsert(query, find_params, update_params, opts \\ []) do
    case find(query, find_params, opts) do
      {:ok, schema_data} ->
        update(query, schema_data, update_params, opts)

      {:error, %{code: :not_found}} ->
        create(query, Map.merge(find_params, update_params), opts)

      {:error, _} = e ->
        e
    end
  end

  @doc group: "Query API"
  @doc """
  Finds a record by the given parameters and deletes it.

  This function combines a `find/3` followed by a `delete/2`. If the
  record is found, it is deleted. If no matching record is found,
  an error is returned.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> EctoShorts.Actions.find_and_delete(MyApp.User, %{id: 1})
      {:ok, %User{}}

      iex> EctoShorts.Actions.find_and_delete({"users", MyApp.User}, %{email: "fira@example.com"}, repo: MyApp.Repo)
      {:ok, %User{}}
  """
  @spec find_and_delete(
          query() | sourceable(),
          params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_and_delete(
          query() | sourceable(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_and_delete(query, find_params, opts \\ []) do
    with {:ok, schema_data} <- find(query, find_params, opts) do
      delete(schema_data, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Finds a record by the given parameters or creates it if not found.

  This function checks if a record exists, and creates it if it does not.

  The same parameter map is used for both the find and create steps.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ### Additional Options

    * `:drop_associations` – When true (default), any schema associations
      in the parameter map will be excluded before the find query. This avoids
      unexpected behavior due to association joins during lookup.

  ## Examples

      iex> EctoShorts.Actions.find_or_create(MyApp.User, %{email: "fira@example.com"})
      {:ok, %User{}}

      iex> EctoShorts.Actions.find_or_create({"users", MyApp.User}, %{email: "fira@example.com"}, repo: MyApp.Repo)
      {:ok, %User{}}
  """
  @spec find_or_create(
          query() | sourceable(),
          params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec find_or_create(
          query() | sourceable(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def find_or_create(query, params, opts \\ []) do
    with {:error, %{code: :not_found}} <-
           find(
             query,
             maybe_filter_queryable_params(params, query, opts),
             opts
           ) do
      query
      |> CommonQuery.source_for_query()
      |> create(params, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Fetches a single record by its primary key.

  This is a convenience wrapper around [`Ecto.Repo.get/3`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3), with added support
  for using a replica repo when provided.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.get/3`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) are also supported.

  ## Examples

      iex> EctoShorts.Actions.get(MyApp.User, 1)
      %User{id: 1}

      iex> EctoShorts.Actions.get(MyApp.User, 1, repo: MyApp.Repo)
      %User{id: 1}

      iex> EctoShorts.Actions.get({"users", MyApp.User}, "abc-123", replica: MyApp.Repo.Replica)
      %User{id: "abc-123"}
  """
  @spec get(
          query() | sourceable(),
          id :: id()
        ) :: schema_data() | nil
  @spec get(
          query() | sourceable(),
          id :: id(),
          opts()
        ) :: schema_data() | nil
  def get(query, id, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    Config.replica!(opts).get(query, id, opts)
  end

  @doc group: "Query API"
  @doc """
  Fetches all records using the given filters and options.

  This is a convenience version of `all/3` that accepts either a map of filters
  or a keyword list that mixes filters with options like `:repo` or `:replica`.

  If a map is given, it is treated as filter parameters.

  If a keyword list is given, all non-option keys are treated as filters, and
  recognized options like `:repo`, `:replica`, `:order_by`, and `:group_by` are extracted.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  ## Examples

      iex> EctoShorts.Actions.all(MyApp.User, %{id: 1})
      [%User{id: 1}]

      iex> EctoShorts.Actions.all(MyApp.User, id: 1, repo: MyApp.Repo)
      [%User{id: 1}]

      iex> EctoShorts.Actions.all(MyApp.User, id: 1, replica: MyApp.Repo.Replica)
      [%User{id: 1}]
  """
  @spec all(query() | sourceable()) :: list(schema_data())
  @spec all(query() | sourceable(), params()) ::
          list(schema_data())
  @spec all(query() | sourceable(), opts()) ::
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
  Fetches all records matching the given query and filter parameters.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  Additional supported options:

    * `:group_by` – Group results by one or more fields.
    * `:order_by` – Sort results by one or more fields.

  All options accepted by [`Ecto.Repo.all/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) are also supported.

  ## Examples

      iex> EctoShorts.Actions.all(MyApp.User, %{role: "admin"})
      [%User{role: "admin"}]

      iex> EctoShorts.Actions.all(MyApp.User, role: "admin", order_by: :inserted_at, repo: MyApp.Repo)
      [%User{role: "admin"}]

      iex> EctoShorts.Actions.all({"users", MyApp.User}, %{role: "admin"}, replica: MyApp.Repo.Replica)
      [%User{role: "admin"}]
  """
  @spec all(
          query() | sourceable(),
          params(),
          opts()
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
  Inserts a new record into the database.

  This function builds a changeset from the given params and inserts it using
  the specified repo.

  If the schema defines `create_changeset/1` and no custom changeset builder
  is specified, it will be used automatically.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options supported by [`Ecto.Repo.insert/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) are also accepted.

  ## Examples

      iex> EctoShorts.Actions.create(MyApp.User, %{name: "Fira"}, repo: MyApp.Repo)
      {:ok, %User{name: "Fira"}}

      iex> EctoShorts.Actions.create({"users", MyApp.User}, %{name: "Fira"}, repo: MyApp.Repo)
      {:ok, %User{name: "Fira"}}
  """
  @spec create(
          sourceable(),
          params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec create(
          sourceable(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def create(query, params, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "Query API"
  @doc """
  Fetches a single record that matches the given parameters.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.one/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:one/2)
  are also supported.

  ## Examples

      iex> EctoShorts.Actions.find(MyApp.User, %{id: 1}, repo: MyApp.Repo)
      {:ok, %User{id: 1}}

      iex> EctoShorts.Actions.find(MyApp.User, %{email: "notfound@example.com"})
      {:error, %{code: :not_found, message: "Record not found.", ...}}

      iex> EctoShorts.Actions.find({"users", MyApp.User}, %{id: 1})
      {:ok, %User{id: 1}}
  """
  @spec find(
          query() | sourceable(),
          params()
        ) :: {:ok, schema_data()} | {:error, any()}
  @spec find(
          query() | sourceable(),
          params(),
          opts()
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

      schema_data ->
        {:ok, schema_data}
    end
  end

  @doc group: "Schema API"
  @doc """
  Updates a record using either an ID or an existing struct.

  This function allows you to provide either:

    * a primary key (e.g. `123`) — the record will be looked up by `id`
    * an existing struct — the update will be applied directly

  The provided `update_params` are passed through the schema's changeset
  logic and then persisted via [`Ecto.Repo.update/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:update/2).

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.update/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:update/2)
  are also supported.

  ## Examples

      # Using a primary key:

      iex> EctoShorts.Actions.update(MyApp.User, 1, %{name: "New Name"})
      {:ok, %User{name: "New Name"}}

      # Using a struct:

      iex> user = %User{id: 1, name: "Old Name"}
      iex> EctoShorts.Actions.update(MyApp.User, user, %{name: "Updated"})
      {:ok, %User{name: "Updated"}}

      # Using a tuple source:

      iex> user = %User{id: 1}
      iex> EctoShorts.Actions.update({"users", MyApp.User}, user, %{name: "Updated"})
      {:ok, %User{name: "Updated"}}
  """
  @spec update(
          query() | sourceable(),
          id() | schema_data(),
          params()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  @spec update(
          query() | sourceable(),
          id() | schema_data(),
          params(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def update(query, id_or_schema_data, update_params, opts \\ [])

  def update(query, id, update_params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, schema_data} <- find(query, %{id: id}, opts) do
      query
      |> CommonSchema.module_for_schema()
      |> update(schema_data, update_params, opts)
    end
  end

  def update(query, schema_data, update_params, opts) when is_list(update_params) do
    update(query, schema_data, Map.new(update_params), opts)
  end

  def update(query, schema_data, update_params, opts) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonSchema.build_changeset(schema_data, update_params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc group: "Schema API"
  @doc """
  Deletes a record given a struct or changeset.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.delete/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2)
  are also supported.

  ## Examples

      iex> EctoShorts.Actions.delete(MyApp.User, 1)
      {:ok, %User{}}

      iex> EctoShorts.Actions.delete(MyApp.User, "abc123")
      {:ok, %User{}}

      iex> EctoShorts.Actions.delete({"users", MyApp.User}, 1)
      {:ok, %User{}}
  """
  @spec delete(schema_data() | changeset() | list(schema_data() | changeset())) ::
          {:ok, list(schema_data())} | {:error, list(changeset())} | {:error, any()}
  @spec delete(
          schema_data() | changeset() | list(schema_data() | changeset()),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def delete(entries, opts \\ [])

  def delete(%_{data: %_{__meta__: %{schema: queryable}}} = changeset, opts) do
    opts = Keyword.merge(default_opts(), opts)

    with {:error, changeset} <-
           queryable
           |> CommonSchema.build_changeset(changeset, %{}, opts)
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

  def delete(%_{__meta__: %{schema: queryable}} = schema_data, opts) do
    opts = Keyword.merge(default_opts(), opts)

    with {:error, changeset} <-
           queryable
           |> CommonSchema.build_changeset(schema_data, %{}, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "Failed to delete record.",
         %{
           query: queryable,
           changeset: changeset,
           schema_data: schema_data
         },
         opts
       )}
    end
  end

  def delete(entries, opts) when is_list(entries) do
    Utils.reduce_all(entries, fn schema_data_or_changeset ->
      delete(schema_data_or_changeset, opts)
    end)
  end

  def delete(query, id) when is_binary(id) or is_integer(id) do
    delete(query, id, [])
  end

  @doc group: "Schema API"
  @doc """
  Deletes a record using its primary key.

  If the record is not found, an error is returned.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.delete/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:delete/2)
  are also supported.

  ## Examples

      iex> EctoShorts.Actions.delete(MyApp.User, 1, repo: MyApp.Repo)
      {:ok, %User{}}

      iex> EctoShorts.Actions.delete(MyApp.User, "abc123", replica: MyApp.Repo.Replica)
      {:ok, %User{}}

      iex> EctoShorts.Actions.delete({"users", MyApp.User}, 1, repo: MyApp.Repo)
      {:ok, %User{}}
  """
  @spec delete(
          query() | sourceable(),
          id(),
          opts()
        ) :: {:ok, schema_data()} | {:error, changeset()} | {:error, any()}
  def delete(query, id, opts) when is_integer(id) or is_binary(id) do
    with {:ok, schema_data} <- find(query, %{id: id}, opts) do
      delete(schema_data, opts)
    end
  end

  @doc group: "Query API"
  @doc """
  Streams all records that match the given filters.

  This returns a lazy enumerable that can be used to iterate over large
  datasets without loading them all into memory at once.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.stream/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:stream/2)
  are also supported.

  ## Examples

      iex> stream = EctoShorts.Actions.stream(MyApp.User, %{role: "admin"}, repo: MyApp.Repo)
      ...> Enum.to_list(stream)
      [%User{}, %User{}]

      iex> stream =
      ...>   EctoShorts.Actions.stream({"users", MyApp.User}, %{active: true}, repo: MyApp.Repo)
      ...> Enum.take(stream, 10)
      [%User{}, ...]
  """
  @spec stream(query() | sourceable()) :: stream()
  @spec stream(query() | sourceable(), params()) :: stream()
  @spec stream(query() | sourceable(), params(), opts()) :: stream()
  def stream(query, params \\ %{}, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc group: "Query API"
  @doc """
  Calculates an aggregate value from a filtered query.

  This function applies the given filter parameters and computes an
  aggregate (like `:count`, `:sum`, or `:avg`) over a specified field.

  ## Supported Aggregates

    * `:count` – count matching rows
    * `:sum` – sum of values in the field
    * `:avg` – average of values in the field
    * `:min` – minimum value in the field
    * `:max` – maximum value in the field

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.aggregate/4`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:aggregate/4)
  are also supported.

  ## Examples

      iex> EctoShorts.Actions.aggregate(MyApp.User, %{active: true}, :count, :id)
      42

      iex> EctoShorts.Actions.aggregate(MyApp.User, %{role: "admin"}, :avg, :login_count, repo: MyApp.Repo)
      5.75
  """
  @spec aggregate(
          query() | sourceable(),
          params(),
          aggregate_options(),
          key()
        ) :: any() | nil
  @spec aggregate(
          query() | sourceable(),
          params(),
          aggregate_options(),
          key(),
          opts()
        ) :: any() | nil
  def aggregate(query, params, aggregate, key, opts \\ []) do
    opts = Keyword.merge(default_opts(), opts)

    query
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, key, opts)
  end

  @doc group: "Transaction API"
  @doc since: "2.5.0"
  @doc """
  Runs a function or `Ecto.Multi` inside a database transaction.

  This function wraps the given operation in a transaction using the
  configured `:repo`. You can pass either a function or a pre-built
  `Ecto.Multi` struct.

  If a function is passed, it can be either:

    * a zero-arity function (`fn -> ... end`)
    * a one-arity function that receives the `repo` as its argument (`fn repo -> ... end`)

  By default, the transaction is automatically rolled back if the
  function executed inside the transaction returns `:error` or
  `{:error, reason}`.

  You can disable this behavior by passing `rollback_on_error: false`.

  All other values will commit the transaction.

  ## Options

  See the [Shared Options](EctoShorts.Actions.html#module-shared-options).

  All options accepted by [`Ecto.Repo.transaction/2`](https://hexdocs.pm/ecto/Ecto.Repo.html#c:transaction/2) are supported.

  ## Examples

      # Run a transactional function:

      iex> EctoShorts.Actions.transaction(fn ->
      ...>   EctoShorts.Actions.create(MyApp.User, %{name: "Jane"})
      ...> end)

      # Use a one-arity function:

      iex> EctoShorts.Actions.transaction(fn repo ->
      ...>   repo.insert!(%MyApp.User{name: "Jane"})
      ...> end)

      # Run a pre-built Ecto.Multi:

      iex> multi = Ecto.Multi.new()
      ...> |> Ecto.Multi.insert(:user, MyApp.User.changeset(%MyApp.User{}, %{name: "Jane"}))
      ...> EctoShorts.Actions.transaction(multi)
  """
  @spec transaction(function() | multi()) ::
          {:ok, any()} | {:error, any()} | multi_failure()
  @spec transaction(function() | multi(), opts()) ::
          {:ok, any()} | {:error, any()} | multi_failure()
  def transaction(fun_or_multi, opts \\ [])

  def transaction(%_{} = multi, opts) do
    opts = Keyword.merge(default_opts(), opts)

    Config.repo!(opts).transaction(multi, opts)
  end

  def transaction(fun, opts) do
    opts = Keyword.merge(default_opts(), opts)

    tx_fun = fn repo ->
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

    case Config.repo!(opts).transaction(tx_fun, opts) do
      {:error, :error} -> :error
      {:ok, :ok} -> :ok
      result -> result
    end
  end

  defp create_changeset(query, params, opts) do
    schema_module = CommonSchema.module_for_schema(query)

    if function_exported?(schema_module, :create_changeset, 1) and
         not Keyword.has_key?(opts, :build_changeset) do
      schema_module.create_changeset(params)
    else
      CommonSchema.build_changeset(query, params, opts)
    end
  end

  defp maybe_filter_queryable_params(params, query, opts) do
    if Keyword.get(opts, :filter_queryable_params, true) do
      filter_queryable_params(params, query)
    else
      params
    end
  end

  defp filter_queryable_params(params, query) do
    query_fields = CommonSchema.module_for_schema(query).__schema__(:query_fields)

    params
    |> Enum.filter(fn {key, _} -> key in query_fields end)
    |> Map.new()
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
