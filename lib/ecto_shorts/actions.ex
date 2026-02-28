defmodule EctoShorts.Actions do
  @moduledoc """
  Provides the primary interface for common database operations.

  Use this module when you need to create, read, update, or delete records
  through a standardized API that handles query building, error wrapping,
  and changeset management automatically.

  ## Quick start

      # Create a record
      {:ok, post} = EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello"}, repo: MyApp.Repo)

      # Find one record
      {:ok, post} = EctoShorts.Actions.find(EctoShorts.Schema.Post, %{id: 1})

      # Fetch all matching records
      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, %{published: true, limit: 10})

      # Update a record
      {:ok, post} = EctoShorts.Actions.update(EctoShorts.Schema.Post, post, %{title: "Updated"})

      # Delete a record
      {:ok, _} = EctoShorts.Actions.delete(post)

  ## Function groups

  * **CRUD** — `all/1-3`, `create/3`, `find/3`, `update/4`, `delete/1-3`,
    `get/3`, `exists?/3`, `stream/3`, `aggregate/5`, `preload/3`, and
    find-and-\* variants.
  * **Bulk** — `insert_all/3`, `update_all/4`, `delete_all/3` (no transactions).
  * **Multi** — `create_many/3`, `update_many/3`, `delete_many/3`,
    `find_many/3`, `find_or_create_many/3`, `find_and_upsert_many/3`
    (all transactional).
  * **Batch** — `batch/5` and `batch_preload/4`.
  * **Transaction** — `transaction/2` and `transact/2`.

  ## Return values

  * Single-record functions return `{:ok, struct}` or `{:error, reason}`
    where `reason` is an `%ErrorMessage{}` or an `Ecto.Changeset`.
  * Bulk functions return `{count, nil | [struct]}` per Ecto conventions.
  * Multi functions return `{:ok, [struct]}` or `{:error, reason}`.

  ## Shared options

  * `:repo` — the `Ecto.Repo` module to use for write operations.
    Defaults to `EctoShorts.Config.repo/0`.
  * `:replica` — the `Ecto.Repo` to use for read operations.
    Falls back to `:repo` when not set.
  * `:changeset` — a 1-, 2-, or 3-arity function that overrides the
    schema's default `changeset/2` when building changesets.
  * `:dynamic_adapter` — a module implementing
    `EctoShorts.Dynamics.Adapter` for custom dynamic expression handling.

  See also `EctoShorts.CommonFilters`, `EctoShorts.CommonChanges`, and
  `EctoShorts.Config`.
  """

  @moduledoc groups: [
               %{title: "CRUD", description: "Single-record create, read, update, and delete operations."},
               %{title: "Bulk", description: "Multi-row operations without transactions."},
               %{title: "Multi", description: "Transactional multi-record operations using Ecto.Multi."},
               %{title: "Batch", description: "Batch lookups and preloads."},
               %{title: "Transaction", description: "Transaction wrappers."}
             ]

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
  @doc since: "3.0.0"
  @doc """
  Preloads associations on the given struct or list of structs.

  `data` is an Ecto schema struct or a list of structs.
  `preloads` is an atom, list, or keyword list of associations to preload.

  Returns the struct(s) with the requested associations loaded. Uses the
  configured replica repo for the database query.

  ## Examples

      post_with_author = EctoShorts.Actions.preload(post, :author)
      posts_with_tags  = EctoShorts.Actions.preload(posts, [:author, :comments])

  ## Options

  See [Ecto.Repo.preload/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:preload/3) for more information.

  See also `all/3` and `EctoShorts.CommonChanges.preload_change_assoc/3`.
  """
  def preload(data, preloads, opts \\ []) do
    Config.replica!(opts).preload(data, preloads, opts)
  end

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Checks if there exists an entry that matches the given filter params.

  `source` is a schema module, `{source, schema}` tuple, or `Ecto.Query`.
  `params` is a map or keyword list of filter params passed to
  `EctoShorts.CommonFilters.convert_params_to_filter/3`.

  Returns `true` if at least one matching record exists, `false` otherwise.
  Uses the configured replica repo.

  ## Examples

      true = EctoShorts.Actions.exists?(EctoShorts.Schema.Post, %{published: true})

  ## Options

  See [Ecto.Repo.exists?/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:exists?/2) for more information.

  See also `find/3` and `all/3`.
  """
  def exists?(source, params, opts \\ []) do
    source
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).exists?(opts)
  end

  @doc group: "CRUD"
  @doc """
  Fetches all entries from the data store for the given queryable.

  Equivalent to `all(queryable, %{}, [])`. Returns a list of structs.

  See also `all/2`, `all/3`, and `find/3`.
  """
  def all(queryable) do
    all(queryable, %{}, [])
  end

  @doc group: "CRUD"
  @doc """
  Fetches all entries from the data store matching the given params or opts.

  When `params` is a map, it is passed as filter params.
  When the second argument is a keyword list, filter keys are extracted
  and `:repo`, `:replica`, and `:dynamic_adapter` are forwarded as options.

  Returns a list of structs.

  ## Examples

      # Pass a map of filter params
      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, %{published: true})

      # Pass opts only (no filter params)
      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, replica: MyApp.Repo)

  See also `all/1`, `all/3`, and `find/3`.
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

  `queryable` is a schema module, `{source, schema}` tuple, or `Ecto.Query`.
  `params` is a map or keyword list of filter params. The `:order_by` and
  `:group_by` keys are also accepted in `opts` and merged into `params`.

  Returns a list of structs.

  ## Examples

      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, %{published: true}, order_by: :title)

  ## Options

  * `:order_by` — forwarded into filter params.
  * `:group_by` — forwarded into filter params.

  See [Ecto.Repo.all/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:all/2) for additional options.

  See also `find/3`, `stream/3`, and `EctoShorts.CommonFilters`.
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
  Creates a new record from the given params.

  `schema` is a schema module or `{source, schema}` tuple.
  `params` is a map of attributes. A changeset is built using the schema's
  `changeset/2` function (or the `:changeset` option if provided) and then
  inserted via the configured repo.

  Returns `{:ok, struct}` on success or `{:error, changeset}` on validation
  failure.

  ## Examples

      iex> EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello", body: "World"}, repo: EctoShorts.Repo)
      {:ok, %EctoShorts.Schema.Post{title: "Hello", body: "World", ...}}

  ## Options

  See [Ecto.Repo.insert/2](https://hexdocs.pm/ecto/Ecto.Repo.html#c:insert/2) for more information.

  See also `find/3`, `update/4`, and `EctoShorts.CommonChanges`.
  """
  def create(schema, params, opts \\ []) do
    schema
    |> CommonSchema.create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "CRUD"
  @doc """
  Fetches a single struct by primary key.

  `queryable` is a schema module or `Ecto.Query`.
  `id` is the primary key value.

  Returns the struct or `nil` if no record is found. Uses the configured
  replica repo.

  ## Examples

      post = EctoShorts.Actions.get(EctoShorts.Schema.Post, 1)

  ## Options

  See [Ecto.Repo.get/3](https://hexdocs.pm/ecto/Ecto.Repo.html#c:get/3) for more information.

  See also `find/3` and `all/3`.
  """
  def get(queryable, id, opts \\ []) do
    Config.replica!(opts).get(queryable, id, opts)
  end

  @doc group: "CRUD"
  @doc """
  Finds a single record matching the given filter params.

  `queryable` is a schema module, `{source, schema}` tuple, or `Ecto.Query`.
  `params` is a map or keyword list of filter params. The `:order_by` and
  `:group_by` keys are also accepted in `opts` and merged into `params`.

  Returns `{:ok, struct}` when exactly one record is found.
  Returns `{:error, %ErrorMessage{code: :not_found}}` when no record matches.
  When `params` is an empty map and `queryable` is not an `Ecto.Query`,
  returns the error immediately without querying.

  ## Options

  * `:order_by` — forwarded into filter params.
  * `:group_by` — forwarded into filter params.

  ## Examples

      iex> EctoShorts.Actions.find(EctoShorts.Schema.Post, %{id: 1})
      {:ok, %EctoShorts.Schema.Post{id: 1, ...}}

      iex> EctoShorts.Actions.find(EctoShorts.Schema.Post, %{id: -1})
      {:error, %ErrorMessage{code: :not_found, message: "record not found."}}

  See also `all/3`, `create/3`, and `find_or_create/3`.
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

  `queryable` is a schema module or `{source, schema}` tuple.
  `id_or_schema_struct` is either an integer/binary id or an Ecto schema
  struct. When an id is given, the record is first fetched with `find/3`.
  `params` is a map of attributes to update.

  Returns `{:ok, updated_struct}` on success, `{:error, changeset}` on
  validation failure, or `{:error, error}` if the record is not found.

  ## Examples

      # Update by id
      {:ok, post} = EctoShorts.Actions.update(EctoShorts.Schema.Post, 1, %{title: "New"}, repo: EctoShorts.Repo)

      # Update by struct
      {:ok, post} = EctoShorts.Actions.update(EctoShorts.Schema.Post, post, %{title: "New"})

  See also `find_and_update/4`, `create/3`, and `EctoShorts.CommonChanges`.
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

  Accepts an Ecto schema struct, an `Ecto.Changeset`, or a list of either.
  A delete changeset is built via the schema's `changeset/2` and then
  deleted through the configured repo.

  Returns `{:ok, struct}` on success. When given a list, returns
  `{:ok, [struct]}` if all succeed, or `{:error, reason}` on the first
  failure (already-deleted entries are not rolled back). Returns
  `{:error, %ErrorMessage{code: :conflict}}` if the delete changeset fails.

  ## Examples

      # Delete by struct
      {:ok, deleted} = EctoShorts.Actions.delete(post)

      # Delete many at once
      {:ok, deleted_list} = EctoShorts.Actions.delete([post1, post2])

  See also `delete/2`, `delete/3`, and `find_and_delete/3`.
  """
  def delete(data) do
    delete(data, [])
  end

  @doc group: "CRUD"
  @doc """
  Deletes a record, changeset, or list of records/changesets with options.

  See `delete/1` for accepted inputs and return values.

  See also `delete/3` and `find_and_delete/3`.
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

  `queryable` is a schema module or `{source, schema}` tuple.
  `id` is an integer or binary primary key. The record is first fetched
  with `find/3`, then deleted.

  Returns `{:ok, struct}` on success, or `{:error, error}` if the record
  is not found or the delete fails.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.delete(EctoShorts.Schema.Post, 1)

  See also `delete/1`, `delete_all/3`, and `find_and_delete/3`.
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
  Streams records matching the given filter params.

  `queryable` is a schema module, `{source, schema}` tuple, or `Ecto.Query`.
  `params` is a map or keyword list of filter params.

  Returns an `Ecto.Repo.stream` result (a stream that must be used inside
  a transaction).

  ## Examples

      EctoShorts.Actions.transact(fn ->
        EctoShorts.Schema.Post
        |> EctoShorts.Actions.stream(%{published: true})
        |> Stream.each(&process_post/1)
        |> Stream.run()
      end)

  See also `all/3`, `transact/2`, and `EctoShorts.CommonFilters`.
  """
  def stream(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc group: "CRUD"
  @doc """
  Applies an aggregate operation to filtered records.

  `queryable` is a schema module, `{source, schema}` tuple, or `Ecto.Query`.
  `params` is a map or keyword list of filter params.
  `aggregate` is the aggregate function (default `:count`).
  `key` is the field to aggregate on (default `:id`).

  Returns the aggregate result (e.g., an integer for `:count`). Uses the
  configured replica repo.

  ## Examples

      count = EctoShorts.Actions.aggregate(EctoShorts.Schema.Post, %{published: true})
      total = EctoShorts.Actions.aggregate(EctoShorts.Schema.Post, %{}, :sum, :views)

  See also `all/3` and `exists?/3`.
  """
  def aggregate(queryable, params \\ %{}, aggregate \\ :count, key \\ :id, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, key, opts)
  end

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Finds a record matching `find_params` and creates one with `create_params` when not found.

  Returns `{:ok, struct}` in both cases (found or created), or
  `{:error, changeset}` if creation fails validation.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_and_create(
        EctoShorts.Schema.Post,
        %{title: "Hello"},
        %{title: "Hello", body: "World"}
      )

  See also `find_or_create/3`, `find_and_update/4`, and `create/3`.
  """
  def find_and_create(queryable, find_params, create_params, opts \\ []) do
    with {:error, _} <- find(queryable, find_params, opts) do
      create(queryable, create_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record matching `find_params` and updates it with `update_params`.

  Returns `{:ok, updated_struct}` on success, `{:error, changeset}` on
  validation failure, or `{:error, error}` if the record is not found.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_and_update(
        EctoShorts.Schema.Post,
        %{id: 1},
        %{title: "Updated"}
      )

  See also `update/4`, `find_and_upsert/4`, and `find/3`.
  """
  def find_and_update(source, find_params, update_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      update(source, record, update_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record matching `find_params` and updates it, or creates a new one.

  When found, updates the record with `upsert_params`. When not found,
  creates a new record by merging `find_params` and `upsert_params`.

  Returns `{:ok, struct}` on success or `{:error, changeset}` on failure.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_and_upsert(
        EctoShorts.Schema.Post,
        %{title: "Hello"},
        %{body: "Updated body"}
      )

  See also `find_and_update/4`, `find_or_create/3`, and `create/3`.
  """
  def find_and_upsert(source, find_params, upsert_params, opts \\ []) do
    case find(source, find_params, opts) do
      {:ok, record} -> update(source, record, upsert_params, opts)
      {:error, _} -> create(source, Map.merge(find_params, upsert_params), opts)
    end
  end

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Finds a record matching `find_params` and deletes it.

  Returns `{:ok, deleted_struct}` on success, or `{:error, error}` if the
  record is not found or the delete fails.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.find_and_delete(EctoShorts.Schema.Post, %{title: "Hello"})

  See also `delete/1`, `delete_all/3`, and `find/3`.
  """
  def find_and_delete(source, find_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      delete(record, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record by query fields or creates it when not found.

  Filters `params` down to the schema's query fields for the find query.
  If no record matches, creates one using the full `params` map.

  Returns `{:ok, struct}` in both cases (found or created), or
  `{:error, changeset}` if creation fails validation.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_or_create(
        EctoShorts.Schema.Post,
        %{title: "Hello", body: "World"}
      )

  See also `find_and_create/4`, `find_or_create_many/3`, and `create/3`.
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
  @doc since: "3.0.0"
  @doc """
  Runs the given function or `Ecto.Multi` in a transaction.

  `fun_or_multi` is either an `Ecto.Multi` struct or a function.

  Returns `{:ok, result}` or `{:error, reason}` per `c:Ecto.Repo.transaction/2` semantics.

  ## Examples

      {:ok, _} = EctoShorts.Actions.transaction(fn ->
        EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello"})
      end)

  See also `transact/2` and `create_many/3`.
  """
  def transaction(fun_or_multi, opts \\ []) do
    Config.repo!(opts).transaction(fun_or_multi, opts)
  end

  @doc group: "Transaction"
  @doc since: "3.0.0"
  @doc """
  Runs a transaction with normalized return values.

  When given an `Ecto.Multi`, runs it and normalizes the multi response
  into `{:ok, [struct]}` or `{:error, reason}`.

  When given a function, wraps it in a transaction. The function can be
  0-arity or 1-arity (receiving the repo). By default (`:strict` is `true`),
  `{:error, reason}` returned from the function triggers a rollback, and
  `{:ok, value}` is unwrapped. Set `strict: false` to return the raw
  transaction result.

  ## Options

  * `:strict` — when `true`, automatically rolls back on `{:error, reason}`
    and unwraps `{:ok, value}`. Defaults to `true`.

  See also `transaction/2` and `create_many/3`.
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
  @doc since: "3.0.0"
  @doc """
  Batches records by key(s) and cardinality.

  `schema` is a schema module or `{source, schema}` tuple.
  `params` is a list of maps or keyword lists used to build batch lookups.
  `batch_keys` is an atom or list of atoms identifying the grouping key(s)
  (default `:id`). `cardinality` is `:one` or `:many` (default `:many`).

  Returns a map where each key is the batch key value (or a map of key
  values for composite keys) and each value is the matching struct (for
  `:one`) or list of structs (for `:many`). Returns `%{}` when `params`
  is empty.

  Raises `ArgumentError` if `:one` cardinality finds multiple records for
  a batch key.
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
  @doc since: "3.0.0"
  @doc """
  Preloads batch lookup records and zips them with original entries.

  `schema` is a schema module or `{source, schema}` tuple.
  `entries` is a list of maps or `{find_params, other_params}` tuples.
  `keys` is an atom, list of atoms, or `true` to use all params as keys.

  Fetches records matching the key fields from `entries` using `batch/5`
  with `:one` cardinality, then zips each fetched record back into the
  corresponding entry. Entries that don't match a record are left unchanged.

  Returns the updated list of entries.
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
  @doc since: "3.0.0"
  @doc """
  Inserts many records from a params list using `c:Ecto.Repo.insert_all/3` semantics.

  `source` is a schema module or `{source, schema}` tuple.
  `params_list` is a list of maps, keyword lists, schema structs,
  `{struct, params}` tuples, or changesets. Each entry is validated through
  the schema's `changeset/2` unless `validate: false` is passed.

  When the `:preload` option is set, matching records are fetched via
  `batch_preload/4` and zipped into the params list before insertion.

  Returns `{:ok, {count, nil | [struct]}}` on success or
  `{:error, [changeset]}` if any entry fails validation.

  ## Options

  * `:preload` — an atom or list of atoms identifying keys to batch-preload.

  * `:validate` — set to `false` to skip changeset validation.

  * `:on_conflict_replace` — controls which fields are replaced on conflict.
    Accepts `:none`, `:insert_keys` (default), or a list of field atoms.

  * `:placeholders` — a map of `{field, match_value}` for placeholder substitution.

  * `:on_placeholder_conflict` — `:nothing` (default), `:replace_all`, or
    `{:replace, [fields]}`.

  See `EctoShorts.CommonParams.convert_to_insert_params/3` for timestamp options.
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
  @doc since: "3.0.0"
  @doc """
  Updates all records matching `find_params` with `update_params`.

  `source` is a schema module or `{source, schema}` tuple.
  `find_params` is a map or keyword list of filter params.
  `update_params` is a map of fields to update, supporting `:set`, `:inc`,
  `:push`, and `:pull` operations.

  Returns `{count, nil}` where `count` is the number of updated rows.

  See also `EctoShorts.CommonParams.convert_to_update_params/3` and `update_many/3`.
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
  @doc since: "3.0.0"
  @doc """
  Deletes all records matching the given filter params.

  `queryable` is a schema module, `{source, schema}` tuple, or `Ecto.Query`.
  `params` is a map or keyword list of filter params.

  Returns `{count, nil}` where `count` is the number of deleted rows.

  See also `delete_many/3` and `EctoShorts.CommonFilters`.
  """
  def delete_all(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Creates many records in a transaction.

  `schema` is a schema module or `{source, schema}` tuple.
  `params_list` is a list of maps, one per record to create.

  Each record is inserted individually inside an `Ecto.Multi`. If any
  insert fails, the entire transaction is rolled back.

  Returns `{:ok, [struct]}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.create_many(EctoShorts.Schema.Post, [
        %{title: "Post 1", body: "Body 1"},
        %{title: "Post 2", body: "Body 2"}
      ])

  See also `create/3`, `insert_all/3`, and `transact/2`.
  """
  def create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_create_many_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Finds many records in a transaction.

  `schema` is a schema module or `{source, schema}` tuple.
  `params_list` is a list of maps, one per record to find.

  Each lookup runs inside an `Ecto.Multi`. If any lookup returns `nil`,
  the transaction is rolled back with a `:not_found` error.

  Returns `{:ok, [struct]}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.find_many(EctoShorts.Schema.Post, [%{id: 1}, %{id: 2}])

  See also `find/3`, `find_or_create_many/3`, and `transact/2`.
  """
  def find_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_find_many_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Updates many records in a transaction.

  `schema` is a schema module or `{source, schema}` tuple.
  `entries` is a list of `{find_params, update_params}` tuples or maps
  with an `:id` key. Each entry is found and then updated inside an
  `Ecto.Multi`.

  Returns `{:ok, [struct]}` on success or `{:error, reason}` on failure.
  Raises `ArgumentError` if an entry is not a recognized shape.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.update_many(EctoShorts.Schema.Post, [
        {%{id: 1}, %{title: "Updated 1"}},
        {%{id: 2}, %{title: "Updated 2"}}
      ])

  See also `update/4`, `find_and_update/4`, and `update_all/4`.
  """
  def update_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> Multi.build_update_many_multi(entries, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Deletes many records in a transaction.

  `schema` is a schema module or `{source, schema}` tuple.
  `records` is a list of schema structs, maps with filter params, or
  raw id values. Each entry is found (if needed) and deleted inside an
  `Ecto.Multi`.

  Returns `{:ok, [struct]}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.delete_many(EctoShorts.Schema.Post, [post1, post2])

  See also `delete/1`, `delete_all/3`, and `transact/2`.
  """
  def delete_many(schema, records, opts \\ []) when is_list(records) do
    schema
    |> Multi.build_delete_many_multi(records, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Finds or creates many records in a transaction.

  `schema` is a schema module or `{source, schema}` tuple.
  `params_list` is a list of maps. For each entry, attempts to find a
  matching record; if not found, creates one using the same params.

  Returns `{:ok, [struct]}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.find_or_create_many(EctoShorts.Schema.Post, [
        %{title: "Post 1", body: "Body 1"},
        %{title: "Post 2", body: "Body 2"}
      ])

  See also `find_or_create/3`, `create_many/3`, and `find_many/3`.
  """
  def find_or_create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_find_or_create_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Finds and upserts many records in a transaction.

  `schema` is a schema module or `{source, schema}` tuple.
  `entries` is a list of `{find_params, upsert_params}` tuples or maps
  with an `:id` key. For each entry, finds a matching record and updates
  it, or creates a new one by merging the params.

  Returns `{:ok, [struct]}` on success or `{:error, reason}` on failure.
  Raises `ArgumentError` if an entry is not a recognized shape.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.find_and_upsert_many(EctoShorts.Schema.Post, [
        {%{id: 1}, %{title: "Updated"}},
        {%{title: "New"}, %{body: "New body"}}
      ])

  See also `find_and_upsert/4`, `update_many/3`, and `find_or_create_many/3`.
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
